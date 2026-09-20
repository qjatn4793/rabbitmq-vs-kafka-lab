package com.beomsoo.mqlab.experiment

import com.rabbitmq.client.Address
import com.rabbitmq.client.MessageProperties
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import com.rabbitmq.client.ConnectionFactory as RabbitConnectionFactory

/**
 * 실험 7 — 브로커 한 대를 강제로 죽였을 때 무슨 일이 일어나는가.
 *
 * 3노드 클러스터에 초당 수십 건씩 계속 발행하면서, 도중에 리더 노드를 docker stop 한다.
 * 측정 대상은 두 가지다.
 *   1) 발행이 몇 초 동안 멈추는가 (가용성)
 *   2) ack 받은 메시지가 사라지지 않는가 (내구성)
 *
 * 실행 순서는 cluster/failover.sh 참고. start 로 발행을 시작해두고 밖에서 노드를 죽인 뒤 result 를 읽는다.
 */
@Service
class FailoverExperiment(
    @Value("\${lab.cluster.kafka-bootstrap}") private val kafkaBootstrap: String,
    @Value("\${lab.cluster.rabbit-host}") private val rabbitHost: String,
    @Value("\${lab.cluster.rabbit-ports}") private val rabbitPorts: String,
) {
    private val runs = ConcurrentHashMap<String, CompletableFuture<Map<String, Any?>>>()

    // ---------------------------------------------------------------- Kafka

    fun startKafka(durationSec: Int, intervalMs: Long): Map<String, Any?> {
        val topic = "exp7-kafka-${System.currentTimeMillis()}"
        admin().use { admin ->
            val newTopic = NewTopic(topic, 1, 3).configs(
                mapOf(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG to "2"),
            )
            admin.createTopics(listOf(newTopic)).all().get()
        }
        val placement = awaitLeader(topic)

        runs["kafka"] = CompletableFuture.supplyAsync { runKafka(topic, durationSec, intervalMs, placement) }

        return mapOf(
            "broker" to "kafka",
            "topic" to topic,
            "replicationFactor" to 3,
            "minInsyncReplicas" to 2,
            "leaderNodeId" to placement.leader,
            "leaderContainer" to "mqlab-kafka${placement.leader}",
            "replicas" to placement.replicas,
            "isr" to placement.isr,
            "durationSec" to durationSec,
            "hint" to "지금 docker stop mqlab-kafka${placement.leader} 를 실행한 뒤 /experiments/7/kafka/result 를 호출하세요",
        )
    }

    private fun runKafka(topic: String, durationSec: Int, intervalMs: Long, before: Placement): Map<String, Any?> {
        val report = Report("EXP7-FAILOVER-KAFKA")
        val timeline = Timeline()
        val acked = AtomicInteger()
        val failed = AtomicInteger()
        val attempted = AtomicInteger()
        val errors = ConcurrentHashMap<String, AtomicInteger>()

        kafkaProducer().use { producer ->
            val start = System.currentTimeMillis()
            val endAt = start + durationSec * 1000L
            var seq = 0
            while (System.currentTimeMillis() < endAt) {
                val second = ((System.currentTimeMillis() - start) / 1000).toInt()
                val sentAt = System.currentTimeMillis()
                attempted.incrementAndGet()
                timeline.attempt(second)
                producer.send(ProducerRecord(topic, null, "${seq++}".toByteArray())) { _, e ->
                    val latency = System.currentTimeMillis() - sentAt
                    if (e == null) {
                        acked.incrementAndGet()
                        timeline.ack(second, latency)
                    } else {
                        failed.incrementAndGet()
                        timeline.fail(second)
                        errors.computeIfAbsent(e.javaClass.simpleName) { AtomicInteger() }.incrementAndGet()
                    }
                }
                Thread.sleep(intervalMs)
            }
            runCatching { producer.flush() }
        }

        val after = runCatching { describe(topic) }.getOrNull()
        val consumed = runCatching { consumeAll(topic) }.getOrDefault(-1)

        report.add("실험 7 (Kafka) — 파티션 리더 브로커를 죽였을 때")
        report.add("토픽 $topic  replication.factor=3, min.insync.replicas=2, partition 1개")
        report.add("장애 전 : leader=${before.leader}, replicas=${before.replicas}, isr=${before.isr}")
        report.add("장애 후 : leader=${after?.leader}, replicas=${after?.replicas}, isr=${after?.isr}")
        report.add("")
        report.add(timeline.render())
        report.add("")
        report.add("발행 시도 ${attempted.get()}건, ack ${acked.get()}건, 실패 ${failed.get()}건")
        report.add("에러 종류 : ${errors.mapValues { it.value.get() }}")
        report.add("재소비 결과 : ${consumed}건")
        report.add("ack 받은 ${acked.get()}건 중 유실 : ${if (consumed >= 0) acked.get() - consumed else -1}건")
        report.add("발행이 멈춘 구간 : ${timeline.outageSeconds()}초")

        return mapOf(
            "broker" to "kafka",
            "topic" to topic,
            "leaderBefore" to before.leader,
            "leaderAfter" to after?.leader,
            "isrBefore" to before.isr,
            "isrAfter" to after?.isr,
            "attempted" to attempted.get(),
            "acked" to acked.get(),
            "failed" to failed.get(),
            "errors" to errors.mapValues { it.value.get() },
            "consumedAfterRecovery" to consumed,
            "lostMessages" to if (consumed >= 0) acked.get() - consumed else null,
            "outageSeconds" to timeline.outageSeconds(),
            "timeline" to timeline.rows(),
            "log" to report.lines,
        )
    }

    // ------------------------------------------------------------- RabbitMQ

    fun startRabbit(durationSec: Int, intervalMs: Long): Map<String, Any?> {
        val queue = "exp7-rabbit-${System.currentTimeMillis()}"
        // quorum queue 를 만들어야 3노드에 복제된다. classic queue 는 한 노드에만 존재한다.
        rabbitConnection().use { connection ->
            connection.createChannel().use { channel ->
                channel.queueDeclare(queue, true, false, false, mapOf("x-queue-type" to "quorum"))
            }
        }
        val leader = quorumLeader(queue)
        runs["rabbit"] = CompletableFuture.supplyAsync { runRabbit(queue, durationSec, intervalMs, leader) }

        return mapOf(
            "broker" to "rabbitmq",
            "queue" to queue,
            "queueType" to "quorum",
            "leaderNode" to leader,
            "leaderContainer" to "mqlab-${leader.substringAfter('@')}",
            "durationSec" to durationSec,
            "hint" to "지금 docker stop mqlab-${leader.substringAfter('@')} 를 실행한 뒤 /experiments/7/rabbit/result 를 호출하세요",
        )
    }

    private fun runRabbit(queue: String, durationSec: Int, intervalMs: Long, leaderBefore: String): Map<String, Any?> {
        val report = Report("EXP7-FAILOVER-RABBIT")
        val timeline = Timeline()
        var attempted = 0
        var acked = 0
        var failed = 0
        val errors = mutableMapOf<String, Int>()

        val connection = rabbitConnection()
        var channel = connection.createChannel().also { it.confirmSelect() }

        val start = System.currentTimeMillis()
        val endAt = start + durationSec * 1000L
        var seq = 0
        while (System.currentTimeMillis() < endAt) {
            val second = ((System.currentTimeMillis() - start) / 1000).toInt()
            val sentAt = System.currentTimeMillis()
            attempted++
            timeline.attempt(second)
            try {
                channel.basicPublish("", queue, MessageProperties.PERSISTENT_BASIC, "${seq++}".toByteArray())
                channel.waitForConfirmsOrDie(3_000)
                acked++
                timeline.ack(second, System.currentTimeMillis() - sentAt)
            } catch (e: Exception) {
                failed++
                timeline.fail(second)
                errors.merge(e.javaClass.simpleName, 1, Int::plus)
                // 채널이 깨졌으면 새로 연다. 자동 복구가 연결을 살려둔다.
                channel = runCatching { connection.createChannel().also { it.confirmSelect() } }.getOrElse { channel }
            }
            Thread.sleep(intervalMs)
        }

        val remaining = runCatching { queueDepth(queue) }.getOrDefault(-1)
        val leaderAfter = runCatching { quorumLeader(queue) }.getOrDefault("unknown")
        runCatching { connection.close() }

        report.add("실험 7 (RabbitMQ) — quorum queue 리더 노드를 죽였을 때")
        report.add("큐 $queue  x-queue-type=quorum (3노드 복제)")
        report.add("장애 전 리더 : $leaderBefore")
        report.add("장애 후 리더 : $leaderAfter")
        report.add("")
        report.add(timeline.render())
        report.add("")
        report.add("발행 시도 ${attempted}건, confirm ${acked}건, 실패 ${failed}건")
        report.add("에러 종류 : $errors")
        report.add("큐에 남아있는 메시지 : ${remaining}건")
        report.add("confirm 받은 ${acked}건 중 유실 : ${if (remaining >= 0) acked - remaining else -1}건")
        report.add("발행이 멈춘 구간 : ${timeline.outageSeconds()}초")

        return mapOf(
            "broker" to "rabbitmq",
            "queue" to queue,
            "leaderBefore" to leaderBefore,
            "leaderAfter" to leaderAfter,
            "attempted" to attempted,
            "acked" to acked,
            "failed" to failed,
            "errors" to errors,
            "remainingInQueue" to remaining,
            "lostMessages" to if (remaining >= 0) acked - remaining else null,
            "outageSeconds" to timeline.outageSeconds(),
            "timeline" to timeline.rows(),
            "log" to report.lines,
        )
    }

    fun result(broker: String): Map<String, Any?> =
        runs[broker]?.get() ?: mapOf("error" to "$broker 실행 기록이 없습니다. 먼저 start 를 호출하세요")

    // ---------------------------------------------------------------- 내부

    private data class Placement(val leader: Int, val replicas: List<Int>, val isr: List<Int>)

    /** 토픽을 만든 직후에는 메타데이터가 아직 퍼지지 않아 조회가 실패한다. 리더가 뽑힐 때까지 기다린다. */
    private fun awaitLeader(topic: String, timeoutMs: Long = 20_000): Placement {
        val deadline = System.currentTimeMillis() + timeoutMs
        var last: Throwable? = null
        while (System.currentTimeMillis() < deadline) {
            val placement = runCatching { describe(topic) }.onFailure { last = it }.getOrNull()
            if (placement != null && placement.leader >= 0 && placement.isr.size >= 2) return placement
            Thread.sleep(500)
        }
        throw IllegalStateException("리더 선출을 ${timeoutMs}ms 안에 확인하지 못했습니다", last)
    }

    private fun describe(topic: String): Placement = admin().use { admin ->
        val info = admin.describeTopics(listOf(topic)).allTopicNames().get().getValue(topic)
        val partition = info.partitions().first()
        Placement(
            leader = partition.leader()?.id() ?: -1,
            replicas = partition.replicas().map { it.id() }.sorted(),
            isr = partition.isr().map { it.id() }.sorted(),
        )
    }

    private fun admin(): Admin =
        Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBootstrap))

    private fun kafkaProducer(): KafkaProducer<String, ByteArray> {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
            // 장애를 빨리 드러내기 위해 타임아웃을 짧게 잡는다
            put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3_000)
            put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2_000)
            put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 6_000)
            put(ProducerConfig.LINGER_MS_CONFIG, 0)
        }
        return KafkaProducer(props)
    }

    private fun consumeAll(topic: String): Int {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
            put(ConsumerConfig.GROUP_ID_CONFIG, "exp7-verify-${System.currentTimeMillis()}")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        }
        KafkaConsumer<String, String>(props).use { consumer ->
            consumer.subscribe(listOf(topic))
            var count = 0
            var emptyPolls = 0
            while (emptyPolls < 5) {
                val records = consumer.poll(Duration.ofMillis(1000)).count()
                if (records == 0) emptyPolls++ else { emptyPolls = 0; count += records }
            }
            return count
        }
    }

    private fun addresses(): List<Address> =
        rabbitPorts.split(",").map { Address(rabbitHost, it.trim().toInt()) }

    private fun rabbitConnection() =
        RabbitConnectionFactory().apply {
            username = "guest"
            password = "guest"
            isAutomaticRecoveryEnabled = true
            networkRecoveryInterval = 1_000
        }.newConnection(addresses())

    private fun quorumLeader(queue: String): String =
        RabbitInsightHttp(rabbitHost).queueLeader(queue)

    /**
     * 큐 적재량은 관리 API 대신 AMQP passive declare 로 읽는다.
     * 관리 API 의 messages 는 통계 수집 주기(기본 5초) 때문에 뒤처져서,
     * 발행 직후에 읽으면 실제보다 적게 나와 "유실"로 오해하게 된다.
     */
    private fun queueDepth(queue: String): Int =
        rabbitConnection().use { connection ->
            connection.createChannel().use { it.queueDeclarePassive(queue).messageCount }
        }

    /** 초 단위 집계 */
    private class Timeline {
        private val attempted = ConcurrentHashMap<Int, AtomicInteger>()
        private val acked = ConcurrentHashMap<Int, AtomicInteger>()
        private val failed = ConcurrentHashMap<Int, AtomicInteger>()
        private val maxLatency = ConcurrentHashMap<Int, AtomicLong>()

        fun attempt(second: Int) = attempted.computeIfAbsent(second) { AtomicInteger() }.incrementAndGet()
        fun ack(second: Int, latencyMs: Long) {
            acked.computeIfAbsent(second) { AtomicInteger() }.incrementAndGet()
            maxLatency.computeIfAbsent(second) { AtomicLong() }.accumulateAndGet(latencyMs, ::maxOf)
        }
        fun fail(second: Int) = failed.computeIfAbsent(second) { AtomicInteger() }.incrementAndGet()

        fun rows(): List<Map<String, Any>> =
            attempted.keys.sorted().map { s ->
                mapOf(
                    "second" to s,
                    "attempted" to (attempted[s]?.get() ?: 0),
                    "acked" to (acked[s]?.get() ?: 0),
                    "failed" to (failed[s]?.get() ?: 0),
                    "maxLatencyMs" to (maxLatency[s]?.get() ?: 0L),
                )
            }

        /** ack 가 한 건도 없던 초의 개수 */
        fun outageSeconds(): Int = attempted.keys.count { (acked[it]?.get() ?: 0) == 0 }

        fun render(): String {
            val header = " 초 | 시도 | ack | 실패 | 최대지연(ms) | 그래프"
            val body = rows().joinToString("\n") { r ->
                val a = r["acked"] as Int
                val bar = if (a == 0) "■ 중단" else "▇".repeat((a / 5).coerceAtLeast(1))
                "%3d | %4d | %3d | %4d | %12d | %s".format(
                    r["second"], r["attempted"], a, r["failed"], r["maxLatencyMs"], bar,
                )
            }
            return "$header\n$body"
        }
    }
}
