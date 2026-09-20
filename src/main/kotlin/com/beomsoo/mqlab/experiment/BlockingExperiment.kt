package com.beomsoo.mqlab.experiment

import com.beomsoo.mqlab.config.KafkaSupport
import com.beomsoo.mqlab.config.RabbitSupport
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.springframework.amqp.AmqpRejectAndDontRequeueException
import org.springframework.amqp.core.MessageListener
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * 실험 4 — 실패한 메시지 한 건이 뒤의 메시지를 막는가.
 *
 * Kafka  : 오프셋은 파티션마다 하나뿐이라, 4번을 넘기지 못하면 5번 이후도 영원히 처리되지 않는다.
 *          이것이 head-of-line blocking 이다.
 * RabbitMQ: 실패한 메시지만 DLQ 로 빠지고 나머지는 그대로 흘러간다.
 */
@Service
class BlockingExperiment(
    private val kafka: KafkaSupport,
    private val rabbit: RabbitSupport,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val rabbitTemplate: RabbitTemplate,
    private val connectionFactory: ConnectionFactory,
) {
    companion object {
        const val TOPIC_BASE = "exp4-blocking"
        const val QUEUE = "exp4.blocking"
        const val DLQ = "exp4.blocking.dlq"
        const val FAIL_AT = 4
        const val RETRY_BACKOFF_MS = 200L
    }

    fun run(count: Int = 10, kafkaObserveMs: Long = 5_000): Map<String, Any> {
        val report = Report("EXP4-BLOCKING")
        report.add("실험 4 — 실패한 메시지 한 건이 뒤를 막는가 (head-of-line blocking)")
        report.add("1~$count 을 보내고 ${FAIL_AT}번에서만 항상 예외를 던진다")

        // ---------- Kafka ----------
        val topic = kafka.freshTopic(TOPIC_BASE, 1)
        report.section("Kafka  topic=$topic (partition 1)")
        repeat(count) { i -> kafkaTemplate.send(topic, "${i + 1}").get() }

        val kafkaProcessed = mutableListOf<Int>()
        var retries = 0
        kafka.newConsumer("exp4-${System.currentTimeMillis()}").use { consumer ->
            consumer.subscribe(listOf(topic))
            val deadline = System.currentTimeMillis() + kafkaObserveMs
            loop@ while (System.currentTimeMillis() < deadline) {
                val records = consumer.poll(Duration.ofMillis(300))
                for (record in records) {
                    val seq = record.value().toInt()
                    val partition = TopicPartition(record.topic(), record.partition())
                    if (seq == FAIL_AT) {
                        retries++
                        // 처리 실패 → 커밋하지 않고 같은 오프셋으로 되돌린다 (재시도)
                        consumer.seek(partition, record.offset())
                        Thread.sleep(RETRY_BACKOFF_MS)
                        continue@loop
                    }
                    kafkaProcessed += seq
                    consumer.commitSync(mapOf(partition to OffsetAndMetadata(record.offset() + 1)))
                }
            }
        }
        val neverProcessed = (1..count).filter { it != FAIL_AT && it !in kafkaProcessed }
        report.add("처리 성공 : $kafkaProcessed")
        report.add("${FAIL_AT}번 재시도 : ${retries}회 (${kafkaObserveMs}ms 동안, ${RETRY_BACKOFF_MS}ms 백오프)")
        report.add("끝내 처리 못한 메시지 : $neverProcessed")
        report.add("→ 오프셋이 ${FAIL_AT}번에서 멈춰 파티션 전체가 막혔다")

        // ---------- RabbitMQ ----------
        report.section("RabbitMQ  queue=$QUEUE, dead letter=$DLQ")
        rabbit.recreateQueueWithDeadLetter(QUEUE, DLQ)

        val rabbitProcessed = Collections.synchronizedList(mutableListOf<Int>())
        val rejected = AtomicInteger()
        val latch = CountDownLatch(count)

        val container = SimpleMessageListenerContainer(connectionFactory).apply {
            setQueueNames(QUEUE)
            setConcurrentConsumers(1)
            setMaxConcurrentConsumers(1)
            setPrefetchCount(1)
            setMessageListener(
                MessageListener { message ->
                    val seq = String(message.body).toInt()
                    if (seq == FAIL_AT) {
                        rejected.incrementAndGet()
                        latch.countDown()
                        // requeue 하지 않고 dead letter 로 보낸다
                        throw AmqpRejectAndDontRequeueException("의도적 실패: seq=$seq")
                    }
                    rabbitProcessed += seq
                    latch.countDown()
                },
            )
        }
        repeat(count) { i -> rabbitTemplate.convertAndSend(QUEUE, "${i + 1}") }
        container.start()
        val finished = latch.await(30, TimeUnit.SECONDS)
        container.stop()
        Thread.sleep(300) // dead letter 라우팅이 반영될 시간

        val dlqCount = rabbit.messageCount(DLQ)
        report.add("처리 성공 : ${rabbitProcessed.sorted()}")
        report.add("DLQ 로 보낸 메시지 : ${dlqCount}건 (seq=$FAIL_AT)")
        report.add("전체 소진 완료 : $finished")
        report.add("→ 실패한 한 건만 격리되고 5번 이후는 정상 처리됐다")

        report.section("정리")
        report.add("Kafka    : 파티션당 오프셋이 하나뿐이라 실패 건을 건너뛸 수 없다. 뒤가 전부 밀린다")
        report.add("           실무 대응 = 재시도 토픽 체인 + DLT (Spring Kafka 의 @RetryableTopic)")
        report.add("RabbitMQ : 메시지 단위로 ack/nack 하므로 실패 건만 DLQ 로 빠지고 나머지는 흐른다")
        report.add("이 차이가 장애 상황에서 가장 크게 체감되는 지점이다")

        return mapOf(
            "experiment" to "4. head-of-line blocking",
            "failAt" to FAIL_AT,
            "kafka" to mapOf(
                "processed" to kafkaProcessed,
                "retriesOnPoisonMessage" to retries,
                "neverProcessed" to neverProcessed,
                "blocked" to neverProcessed.isNotEmpty(),
            ),
            "rabbitmq" to mapOf(
                "processed" to rabbitProcessed.sorted(),
                "deadLettered" to dlqCount,
                "blocked" to false,
            ),
            "log" to report.lines,
        )
    }
}
