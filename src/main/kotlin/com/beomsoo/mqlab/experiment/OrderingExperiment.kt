package com.beomsoo.mqlab.experiment

import com.beomsoo.mqlab.config.RabbitSupport
import org.springframework.amqp.core.MessageListener
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer
import org.springframework.stereotype.Service
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/**
 * 실험 2 — 하나의 큐에 컨슈머를 여러 개 붙이면 순서가 깨지는가.
 *
 * 큐는 FIFO 로 "전달"하지만, 전달받은 뒤 처리 시간이 서로 다르면
 * "처리 완료 순서"는 발행 순서와 달라진다. 컨슈머 1개일 때와 비교한다.
 */
@Service
class OrderingExperiment(
    private val rabbit: RabbitSupport,
    private val rabbitTemplate: RabbitTemplate,
    private val connectionFactory: ConnectionFactory,
) {
    companion object {
        const val QUEUE = "exp2.ordering"
    }

    fun run(count: Int = 30, consumers: Int = 3): Map<String, Any> {
        val report = Report("EXP2-ORDERING")
        report.add("실험 2 — 1개 큐에 컨슈머를 여러 개 붙이면 순서가 보장되는가")
        report.add("메시지 1~$count 을 순서대로 발행하고, 처리 완료 순서를 기록한다")
        report.add("각 메시지 처리 시간은 10~60ms 사이 난수 (실무처럼 제각각인 상황을 흉내)")

        report.section("A. 컨슈머 ${consumers}개 (경쟁 컨슈머)")
        val multi = consume(count, consumers)
        report.add("처리 완료 순서: ${multi.completionOrder}")
        report.add("순서 뒤바뀜(인접 역전): ${multi.inversions}회 → ${if (multi.ordered) "순서 유지됨" else "순서 깨짐"}")

        report.section("B. 컨슈머 1개")
        val single = consume(count, 1)
        report.add("처리 완료 순서: ${single.completionOrder}")
        report.add("순서 뒤바뀜(인접 역전): ${single.inversions}회 → ${if (single.ordered) "순서 유지됨" else "순서 깨짐"}")

        report.section("정리")
        report.add("큐 자체는 FIFO 로 꺼내주지만, 여러 컨슈머가 동시에 처리하면 완료 순서는 보장되지 않는다")
        report.add("RabbitMQ 에서 순서를 지키려면 1큐 1컨슈머, 또는 x-single-active-consumer 를 써야 한다")
        report.add("즉 RabbitMQ 에서는 '순서'와 '병렬 처리'가 맞바꿈 관계다")

        return mapOf(
            "experiment" to "2. ordering",
            "messages" to count,
            "multiConsumer" to mapOf(
                "consumers" to consumers,
                "ordered" to multi.ordered,
                "inversions" to multi.inversions,
                "completionOrder" to multi.completionOrder,
            ),
            "singleConsumer" to mapOf(
                "consumers" to 1,
                "ordered" to single.ordered,
                "inversions" to single.inversions,
                "completionOrder" to single.completionOrder,
            ),
            "log" to report.lines,
        )
    }

    private fun consume(count: Int, concurrency: Int): Outcome {
        rabbit.recreateQueue(QUEUE)
        repeat(count) { i -> rabbitTemplate.convertAndSend(QUEUE, (i + 1).toString()) }

        val completed = Collections.synchronizedList(mutableListOf<Int>())
        val latch = CountDownLatch(count)

        val container = SimpleMessageListenerContainer(connectionFactory).apply {
            setQueueNames(QUEUE)
            setConcurrentConsumers(concurrency)
            setMaxConcurrentConsumers(concurrency)
            setPrefetchCount(1)
            setMessageListener(
                MessageListener { message ->
                    val seq = String(message.body).toInt()
                    Thread.sleep(Random.nextLong(10, 60))
                    completed += seq
                    latch.countDown()
                },
            )
        }

        container.start()
        latch.await(60, TimeUnit.SECONDS)
        container.stop()

        val order = completed.toList()
        val inversions = order.zipWithNext().count { (a, b) -> a > b }
        return Outcome(order, inversions, inversions == 0)
    }

    private data class Outcome(
        val completionOrder: List<Int>,
        val inversions: Int,
        val ordered: Boolean,
    )
}
