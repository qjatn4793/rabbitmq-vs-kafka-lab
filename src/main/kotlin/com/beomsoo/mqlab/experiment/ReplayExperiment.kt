package com.beomsoo.mqlab.experiment

import com.beomsoo.mqlab.config.KafkaSupport
import com.beomsoo.mqlab.config.RabbitSupport
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.time.Duration

/**
 * 실험 1 — 이미 소비한 메시지를 다시 읽을 수 있는가.
 *
 * Kafka  : 컨슈머가 읽어도 로그에 남아 있으므로 오프셋을 되감으면 재소비된다.
 * RabbitMQ: ack 를 보내는 순간 큐에서 제거되므로 되돌릴 방법이 없다.
 */
@Service
class ReplayExperiment(
    private val kafka: KafkaSupport,
    private val rabbit: RabbitSupport,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val rabbitTemplate: RabbitTemplate,
) {
    companion object {
        const val TOPIC_BASE = "exp1-replay"
        const val QUEUE = "exp1.replay"
    }

    fun run(count: Int = 10): Map<String, Any> {
        val report = Report("EXP1-REPLAY")
        report.add("실험 1 — 소비한 메시지를 다시 읽을 수 있는가 (replay)")
        report.add("메시지 $count 건을 보내고, 한 번 소비한 뒤 다시 읽어본다")

        // ---------- Kafka ----------
        val topic = kafka.freshTopic(TOPIC_BASE, 1)
        report.section("Kafka  topic=$topic (partition 1)")
        repeat(count) { i -> kafkaTemplate.send(topic, "msg-${i + 1}").get() }
        report.add("produce 완료 : msg-1 ~ msg-$count")

        var kafkaReplayed = 0
        kafka.newConsumer("exp1-${System.currentTimeMillis()}").use { consumer ->
            consumer.subscribe(listOf(topic))

            val first = consumer.drain(count)
            report.add("1차 consume  : ${first.size}건, offset ${first.map { it.offset() }}")
            consumer.commitSync()
            report.add("offset commit: 완료")

            val again = consumer.poll(Duration.ofMillis(1000)).count()
            report.add("그대로 재-poll: ${again}건  (읽은 지점 뒤에는 남은 게 없음)")

            consumer.seekToBeginning(consumer.assignment())
            val replayed = consumer.drain(count)
            kafkaReplayed = replayed.size
            report.add("seekToBeginning 후 consume: ${replayed.size}건 → ${replayed.map { it.value() }}")
            report.add("결과: 로그가 그대로 남아 있어 같은 메시지를 다시 처리할 수 있다")
        }

        // ---------- RabbitMQ ----------
        report.section("RabbitMQ  queue=$QUEUE")
        rabbit.recreateQueue(QUEUE)
        repeat(count) { i -> rabbitTemplate.convertAndSend(QUEUE, "msg-${i + 1}") }
        report.add("publish 완료 : msg-1 ~ msg-$count")

        val received = buildList {
            repeat(count) {
                (rabbitTemplate.receiveAndConvert(QUEUE, 2000) as String?)?.let { add(it) }
            }
        }
        report.add("1차 consume  : ${received.size}건 (수신과 동시에 ack)")

        val leftover = rabbit.messageCount(QUEUE)
        val secondTry = rabbitTemplate.receiveAndConvert(QUEUE, 1000)
        report.add("큐에 남은 수  : ${leftover}건")
        report.add("재-consume 시도: ${secondTry?.toString() ?: "없음"} → ack 된 메시지는 큐에서 사라져 되돌릴 수 없다")

        report.section("정리")
        report.add("Kafka    : 소비해도 로그는 남는다. retention 기간 안에서는 몇 번이든 재처리 가능")
        report.add("RabbitMQ : ack = 삭제. 재처리하려면 발행 측이 다시 보내주어야 한다")
        report.add("주의     : Kafka 의 replay 도 무한하지 않다. retention.ms(기본 7일)를 넘기면 사라진다")

        return mapOf(
            "experiment" to "1. replay",
            "kafka" to mapOf("produced" to count, "consumed" to count, "replayedAfterSeek" to kafkaReplayed),
            "rabbitmq" to mapOf("produced" to count, "consumed" to received.size, "remainingAfterAck" to leftover),
            "log" to report.lines,
        )
    }

    /** 원하는 건수가 모일 때까지(또는 타임아웃까지) poll 한다. */
    private fun KafkaConsumer<String, String>.drain(
        expected: Int,
        timeoutMs: Long = 5_000,
    ): List<ConsumerRecord<String, String>> {
        val collected = mutableListOf<ConsumerRecord<String, String>>()
        val deadline = System.currentTimeMillis() + timeoutMs
        while (collected.size < expected && System.currentTimeMillis() < deadline) {
            poll(Duration.ofMillis(300)).forEach { collected += it }
        }
        return collected
    }
}
