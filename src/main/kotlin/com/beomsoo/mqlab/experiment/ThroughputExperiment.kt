package com.beomsoo.mqlab.experiment

import com.beomsoo.mqlab.config.KafkaSupport
import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.DeliverCallback
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * 실험 5 — 같은 조건에서 처리량이 얼마나 차이 나는가.
 *
 * "Kafka 가 빠르다" 는 말은 조건을 빼면 의미가 없다.
 * 내구성 우선 / 속도 우선 두 프로파일을 동일하게 맞춰 발행과 소비를 각각 잰다.
 */
@Service
class ThroughputExperiment(
    private val kafka: KafkaSupport,
    @Value("\${spring.rabbitmq.host}") private val rabbitHost: String,
) {
    fun run(count: Int = 100_000, sizeBytes: Int = 512): Map<String, Any> {
        val report = Report("EXP5-THROUGHPUT")
        val totalMb = count.toLong() * sizeBytes / (1024 * 1024)
        report.add("실험 5 — 처리량 비교")
        report.add("메시지 ${count}건 × ${sizeBytes}바이트 = 약 ${totalMb}MB 를 발행하고 다시 소비한다")
        report.add("내구성 우선 : Kafka acks=all + 멱등, RabbitMQ persistent + publisher confirms")
        report.add("속도 우선   : Kafka acks=1, RabbitMQ transient + confirms 없음")

        val payload = Bench.payload(sizeBytes)
        val results = linkedMapOf<String, Map<String, Any>>()

        // ---------- Kafka ----------
        report.section("Kafka")
        val durableTopic = kafka.freshTopic("exp5-kafka-durable", 3)
        results["Kafka 발행 (내구성 우선)"] = kafkaProduce(durableTopic, payload, count, Bench.Durability.DURABLE)
        results["Kafka 소비"] = kafkaConsume(durableTopic, count, sizeBytes)

        val fastTopic = kafka.freshTopic("exp5-kafka-fast", 3)
        results["Kafka 발행 (속도 우선)"] = kafkaProduce(fastTopic, payload, count, Bench.Durability.FAST)

        // ---------- RabbitMQ ----------
        report.section("RabbitMQ")
        val durableQueue = "exp5-rabbit-durable-${System.currentTimeMillis()}"
        results["RabbitMQ 발행 (내구성 우선)"] = rabbitProduce(durableQueue, payload, count, Bench.Durability.DURABLE)
        results["RabbitMQ 소비"] = rabbitConsume(durableQueue, count, sizeBytes)

        val fastQueue = "exp5-rabbit-fast-${System.currentTimeMillis()}"
        results["RabbitMQ 발행 (속도 우선)"] = rabbitProduce(fastQueue, payload, count, Bench.Durability.FAST)
        deleteQueue(fastQueue)

        report.section("결과  (메시지 ${sizeBytes}바이트 기준)")
        results.forEach { (name, stats) -> report.add(Bench.format(name, stats)) }

        val kafkaDurable = results.getValue("Kafka 발행 (내구성 우선)")["msgPerSec"] as Long
        val rabbitDurable = results.getValue("RabbitMQ 발행 (내구성 우선)")["msgPerSec"] as Long
        val ratio = if (rabbitDurable > 0) kafkaDurable.toDouble() / rabbitDurable else 0.0

        report.section("정리")
        report.add("내구성을 같은 수준으로 맞췄을 때 발행 처리량은 Kafka 가 약 %.1f배".format(ratio))
        report.add("Kafka 가 빠른 이유는 마법이 아니라 구조다")
        report.add("  - 메시지를 건건이 다루지 않고 배치로 묶어 파일에 순차 append 한다")
        report.add("  - 라우팅 판단이 없다. 어느 파티션에 붙일지만 정하면 끝")
        report.add("RabbitMQ 는 메시지마다 라우팅하고 상태를 관리한다. 그 대신 유연한 라우팅을 얻는다")
        report.add("주의: 단일 노드 로컬 환경이다. 복제(replication factor 3)를 켜면 Kafka 쪽 수치도 내려간다")

        return mapOf(
            "experiment" to "5. throughput",
            "messages" to count,
            "messageSizeBytes" to sizeBytes,
            "results" to results,
            "kafkaOverRabbitDurableRatio" to String.format("%.2f", ratio).toDouble(),
            "log" to report.lines,
        )
    }

    private fun kafkaProduce(
        topic: String,
        payload: ByteArray,
        count: Int,
        durability: Bench.Durability,
    ): Map<String, Any> {
        Bench.kafkaProducer(kafka.bootstrapServers, durability).use { producer ->
            val start = System.currentTimeMillis()
            repeat(count) { producer.send(ProducerRecord(topic, null, payload)) }
            producer.flush()
            return Bench.rate(count, payload.size, System.currentTimeMillis() - start)
        }
    }

    private fun kafkaConsume(topic: String, count: Int, sizeBytes: Int): Map<String, Any> {
        kafka.newConsumer("exp5-${System.currentTimeMillis()}").use { consumer ->
            consumer.subscribe(listOf(topic))
            var received = 0
            val start = System.currentTimeMillis()
            val deadline = start + 120_000
            while (received < count && System.currentTimeMillis() < deadline) {
                received += consumer.poll(Duration.ofMillis(500)).count()
            }
            return Bench.rate(received, sizeBytes, System.currentTimeMillis() - start)
        }
    }

    private fun rabbitProduce(
        queue: String,
        payload: ByteArray,
        count: Int,
        durability: Bench.Durability,
    ): Map<String, Any> {
        Bench.rabbitConnection(rabbitHost).use { connection ->
            connection.createChannel().use { channel ->
                channel.queueDeclare(queue, true, false, false, null)
                if (durability == Bench.Durability.DURABLE) Bench.enableConfirms(channel)
                val start = System.currentTimeMillis()
                Bench.rabbitPublish(channel, queue, payload, count, durability)
                return Bench.rate(count, payload.size, System.currentTimeMillis() - start)
            }
        }
    }

    private fun rabbitConsume(queue: String, count: Int, sizeBytes: Int): Map<String, Any> {
        Bench.rabbitConnection(rabbitHost).use { connection ->
            connection.createChannel().use { channel ->
                channel.basicQos(500)
                val latch = CountDownLatch(count)
                val start = System.currentTimeMillis()
                val tag = channel.basicConsume(
                    queue,
                    false,
                    DeliverCallback { _, delivery ->
                        channel.basicAck(delivery.envelope.deliveryTag, false)
                        latch.countDown()
                    },
                    CancelCallback { },
                )
                latch.await(120, TimeUnit.SECONDS)
                val elapsed = System.currentTimeMillis() - start
                channel.basicCancel(tag)
                channel.queueDelete(queue)
                return Bench.rate(count - latch.count.toInt(), sizeBytes, elapsed)
            }
        }
    }

    private fun deleteQueue(queue: String) {
        Bench.rabbitConnection(rabbitHost).use { connection ->
            connection.createChannel().use { it.queueDelete(queue) }
        }
    }
}
