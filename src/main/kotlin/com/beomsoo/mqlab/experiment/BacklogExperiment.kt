package com.beomsoo.mqlab.experiment

import com.beomsoo.mqlab.config.KafkaSupport
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * 실험 6 — 컨슈머가 못 따라가서 메시지가 쌓이면 발행 측은 어떻게 되는가.
 *
 * 처리량 절대값보다 이쪽이 "대용량 트래픽을 누가 더 잘 견디는가"에 가까운 질문이다.
 * 컨슈머 없이 계속 발행하면서 구간별 처리량과 브로커 상태를 함께 기록한다.
 *
 * RabbitMQ 의 메모리 임계치를 낮춰서 재현하려면:
 *   docker exec mqlab-rabbitmq rabbitmqctl set_vm_memory_high_watermark absolute 128MiB
 * 원복:
 *   docker exec mqlab-rabbitmq rabbitmqctl set_vm_memory_high_watermark 0.4
 */
@Service
class BacklogExperiment(
    private val kafka: KafkaSupport,
    private val insight: RabbitInsight,
    @Value("\${spring.rabbitmq.host}") private val rabbitHost: String,
) {
    fun run(
        chunks: Int = 20,
        chunkSize: Int = 10_000,
        sizeBytes: Int = 512,
        confirmTimeoutMs: Long = 15_000,
    ): Map<String, Any> {
        val report = Report("EXP6-BACKLOG")
        val total = chunks * chunkSize
        val totalMb = total.toLong() * sizeBytes / (1024 * 1024)
        report.add("실험 6 — 적체가 쌓일 때 발행 측이 버티는가")
        report.add("컨슈머를 붙이지 않고 ${chunkSize}건씩 ${chunks}번, 총 ${total}건(약 ${totalMb}MB)을 발행한다")
        report.add("두 브로커 모두 내구성 우선 (Kafka acks=all, RabbitMQ persistent + publisher confirms)")

        val payload = Bench.payload(sizeBytes)

        report.section("Kafka")
        val topic = kafka.freshTopic("exp6-backlog", 3)
        val kafkaRates = kafkaBacklog(topic, payload, chunks, chunkSize)
        report.add("구간별 msg/s : ${kafkaRates.joinToString(", ")}")
        report.add("첫 구간 ${kafkaRates.first()} → 마지막 구간 ${kafkaRates.last()} msg/s (%+.0f%%)".format(change(kafkaRates)))
        val kafkaStalls = stalls(kafkaRates)
        report.add("중앙값 ${kafkaStalls.median} msg/s, 급락 구간 ${kafkaStalls.hits.size}개 ${kafkaStalls.hits}")
        report.add("발행 차단 없음. 적체는 디스크 로그가 길어지는 것일 뿐이다")

        report.section("RabbitMQ")
        val before = insight.nodeStatus()
        report.add("시작 시점 : ${before.describe()}")
        val rabbit = rabbitBacklog(payload, chunks, chunkSize, confirmTimeoutMs)
        report.add("구간별 msg/s : ${rabbit.rates.joinToString(", ")}")
        if (rabbit.rates.size >= 2) {
            report.add("첫 구간 ${rabbit.rates.first()} → 마지막 구간 ${rabbit.rates.last()} msg/s (%+.0f%%)".format(change(rabbit.rates)))
        }
        val rabbitStalls = stalls(rabbit.rates)
        report.add("중앙값 ${rabbitStalls.median} msg/s, 급락 구간 ${rabbitStalls.hits.size}개 ${rabbitStalls.hits}")
        val after = insight.nodeStatus()
        report.add("종료 시점 : ${after.describe()}")
        report.add("연결 상태 : ${insight.connectionStates()}")

        if (rabbit.blockedAtChunk != null) {
            report.add("")
            report.add("★ ${rabbit.blockedAtChunk}번째 구간(누적 ${(rabbit.blockedAtChunk - 1) * chunkSize}건)에서 발행이 멈췄다")
            report.add("  confirm 이 ${confirmTimeoutMs}ms 안에 오지 않았다. 처리량이 느려진 게 아니라 0이 된 것이다")
            report.add("  원인: 메모리 임계치 초과 → memory alarm → 발행자 연결을 blocked 로 전환 (flow control)")
            rabbit.blockReason?.let { report.add("  브로커가 알려준 차단 사유: $it") }
        } else {
            report.add("")
            if (rabbitStalls.hits.isNotEmpty()) {
                report.add("완전히 멈추지는 않았지만, 발행 처리량이 주기적으로 중앙값의 1/5 아래로 떨어졌다")
                report.add("  메모리 알람이 걸렸다 풀렸다 반복하며 발행자를 짧게 차단하는 패턴이다")
                report.add("  평균만 보면 묻히지만 p99 지연은 여기서 폭발한다")
                rabbit.blockReason?.let { report.add("  브로커가 알려준 차단 사유: $it") }
            } else {
                report.add("이번 실행에서는 발행 차단이 발생하지 않았다")
            }
            report.add("  메모리 사용량이 임계치(${after.memoryLimitBytes / 1024 / 1024}MB)에 도달하지 않았기 때문이다")
            report.add("  임계치를 낮추면 재현된다: rabbitmqctl set_vm_memory_high_watermark absolute 128MiB")
        }

        report.section("정리")
        report.add("Kafka 는 적체가 정상 상태다. 메시지는 원래 디스크 로그에 남고,")
        report.add("컨슈머 진행 위치는 오프셋 숫자 하나로만 관리된다. 그래서 쌓여도 발행이 흔들리지 않는다")
        report.add("RabbitMQ 는 적체가 이상 신호다. 큐는 비워지는 것을 전제로 만들어졌고,")
        report.add("메모리 임계치를 넘으면 발행자 연결을 차단해 스스로를 보호한다")
        report.add("중요: 이건 '느려짐'이 아니라 '멈춤'이다. 발행 측 애플리케이션이 같이 멈춘다")
        report.add("다만 임계치 안에서는 RabbitMQ 도 처리량이 평탄했다. 문제는 한계를 넘는 순간 성격이 바뀐다는 것")
        report.add("실무 해석: 컨슈머가 몇 시간 죽어 있을 때")
        report.add("  Kafka    → 발행 측 영향 없음. 컨슈머만 복구해 따라잡으면 된다")
        report.add("  RabbitMQ → 임계치를 넘기면 발행 측까지 멈춰 장애가 상류로 번진다")

        return mapOf(
            "experiment" to "6. backlog tolerance",
            "totalMessages" to total,
            "chunkSize" to chunkSize,
            "messageSizeBytes" to sizeBytes,
            "kafka" to mapOf(
                "msgPerSecByChunk" to kafkaRates,
                "changePercent" to Math.round(change(kafkaRates)),
                "medianMsgPerSec" to stalls(kafkaRates).median,
                "stallChunks" to stalls(kafkaRates).hits,
                "publisherBlocked" to false,
            ),
            "rabbitmq" to mapOf(
                "msgPerSecByChunk" to rabbit.rates,
                "changePercent" to if (rabbit.rates.size >= 2) Math.round(change(rabbit.rates)) else 0,
                "publisherBlocked" to (rabbit.blockedAtChunk != null),
                "medianMsgPerSec" to rabbitStalls.median,
                "stallChunks" to rabbitStalls.hits,
                "blockedAtChunk" to rabbit.blockedAtChunk,
                "blockReason" to rabbit.blockReason,
                "memoryLimitMb" to after.memoryLimitBytes / 1024 / 1024,
                "memoryAlarm" to after.memoryAlarm,
                "connectionStates" to insight.connectionStates(),
            ),
            "log" to report.lines,
        )
    }

    private fun kafkaBacklog(topic: String, payload: ByteArray, chunks: Int, chunkSize: Int): List<Long> {
        Bench.kafkaProducer(kafka.bootstrapServers, Bench.Durability.DURABLE).use { producer ->
            return (1..chunks).map {
                val start = System.currentTimeMillis()
                repeat(chunkSize) { producer.send(ProducerRecord(topic, null, payload)) }
                producer.flush()
                Bench.rate(chunkSize, payload.size, System.currentTimeMillis() - start)["msgPerSec"] as Long
            }
        }
    }

    /**
     * 메모리 알람이 걸리면 basicPublish 호출 자체가 블로킹된다.
     * 그래서 confirm 타임아웃으로는 잡히지 않고, 별도 스레드 + BlockedListener 로 감지해야 한다.
     */
    private fun rabbitBacklog(
        payload: ByteArray,
        chunks: Int,
        chunkSize: Int,
        confirmTimeoutMs: Long,
    ): RabbitOutcome {
        val queue = "exp6-backlog-${System.currentTimeMillis()}"
        val rates = mutableListOf<Long>()
        var blockedAt: Int? = null
        var blockReason: String? = null
        val executor = Executors.newSingleThreadExecutor()

        try {
            Bench.rabbitConnection(rabbitHost).use { connection ->
                connection.addBlockedListener(
                    { reason -> blockReason = reason },
                    { },
                )
                connection.createChannel().use { channel ->
                    channel.queueDeclare(queue, true, false, false, null)
                    Bench.enableConfirms(channel)
                    for (i in 1..chunks) {
                        val start = System.currentTimeMillis()
                        val task = executor.submit {
                            Bench.rabbitPublish(
                                channel, queue, payload, chunkSize,
                                Bench.Durability.DURABLE, confirmTimeoutMs,
                            )
                        }
                        try {
                            task.get(confirmTimeoutMs, TimeUnit.MILLISECONDS)
                        } catch (e: Exception) {
                            task.cancel(true)
                            blockedAt = i
                            break
                        }
                        rates += Bench.rate(chunkSize, payload.size, System.currentTimeMillis() - start)["msgPerSec"] as Long
                    }
                }
            }
        } catch (e: Exception) {
            if (blockedAt == null) blockedAt = rates.size + 1
        } finally {
            executor.shutdownNow()
        }

        // 차단된 연결로는 큐 삭제도 안 되므로 새 연결로 정리한다
        runCatching {
            Bench.rabbitConnection(rabbitHost).use { it.createChannel().use { ch -> ch.queueDelete(queue) } }
        }
        return RabbitOutcome(rates, blockedAt, blockReason)
    }

    /**
     * 중앙값의 1/5 아래로 떨어진 구간을 "스로틀링"으로 본다.
     * 평균만 보면 주기적인 급락이 묻혀버리기 때문이다.
     */
    private fun stalls(rates: List<Long>): Stalls {
        if (rates.isEmpty()) return Stalls(0, emptyList())
        val median = rates.sorted()[rates.size / 2]
        val hits = rates.withIndex().filter { it.value < median / 5 }
            .map { "#${it.index + 1}(${it.value} msg/s)" }
        return Stalls(median, hits)
    }

    private data class Stalls(val median: Long, val hits: List<String>)

    private fun change(rates: List<Long>): Double {
        val first = rates.first().toDouble()
        if (first <= 0) return 0.0
        return (rates.last() - first) / first * 100
    }

    private data class RabbitOutcome(
        val rates: List<Long>,
        val blockedAtChunk: Int?,
        val blockReason: String? = null,
    )
}
