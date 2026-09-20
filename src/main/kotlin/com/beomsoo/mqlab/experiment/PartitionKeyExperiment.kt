package com.beomsoo.mqlab.experiment

import com.beomsoo.mqlab.config.KafkaSupport
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

/**
 * 실험 3 — Kafka 의 순서 보장은 "파티션 안에서만" 성립한다.
 *
 * 처음 세운 가설: "키 없이 보내면 메시지가 파티션에 흩어져 순서가 깨진다"
 * 12건만 보내봤더니 전부 같은 파티션에 들어갔다. sticky partitioner 때문이다.
 * 소량과 대량을 나눠 측정해서 언제 흩어지는지 확인한다.
 */
@Service
class PartitionKeyExperiment(
    private val kafka: KafkaSupport,
    private val kafkaTemplate: KafkaTemplate<String, String>,
) {
    companion object {
        const val PARTITIONS = 3
        const val PAYLOAD_PADDING = 180 // 레코드를 약 200바이트로 키워 batch.size(기본 16KB)를 여러 번 넘기게 한다
        val ORDERS = listOf("order-A", "order-B", "order-C")
    }

    fun run(eventsPerOrder: Int = 200): Map<String, Any> {
        val report = Report("EXP3-PARTITION-KEY")
        report.add("실험 3 — 키 없이 보내면 정말 순서가 깨지는가")
        report.add("주문 ${ORDERS.size}건의 이벤트를 파티션 ${PARTITIONS}개 토픽에 발행하고 파티션 배치를 관찰한다")
        report.add("레코드 1건은 약 ${PAYLOAD_PADDING + 20}바이트, 프로듀서 batch.size 기본값은 16384바이트")

        report.section("A-1. 키 없이 소량 발행 (주문당 4건, 총 ${ORDERS.size * 4}건)")
        val small = publish(useKey = false, eventsPerOrder = 4)
        report.add("topic=${small.topic}")
        small.partitionsByOrder.forEach { (order, partitions) ->
            report.add("$order : 파티션 ${partitions.sorted()} 사용 (${partitions.size}개)")
        }
        report.add("파티션 분포 : ${small.distribution.toSortedMap()}")
        report.add("→ 키가 없는데도 한 파티션에 몰렸다. 가설과 다르다")

        report.section("A-2. 키 없이 대량 발행 (주문당 ${eventsPerOrder}건, 총 ${ORDERS.size * eventsPerOrder}건)")
        val bulk = publish(useKey = false, eventsPerOrder = eventsPerOrder)
        report.add("topic=${bulk.topic}")
        bulk.partitionsByOrder.forEach { (order, partitions) ->
            report.add("$order : 파티션 ${partitions.sorted()} 사용 (${partitions.size}개)")
        }
        report.add("파티션 분포 : ${bulk.distribution.toSortedMap()}")
        val scattered = bulk.partitionsByOrder.count { it.value.size > 1 }
        report.add("→ 한 주문의 이벤트가 여러 파티션에 흩어진 경우: $scattered / ${ORDERS.size}건")

        report.section("B. 주문 ID 를 키로 발행 (주문당 ${eventsPerOrder}건)")
        val keyed = publish(useKey = true, eventsPerOrder = eventsPerOrder)
        report.add("topic=${keyed.topic}")
        keyed.partitionsByOrder.forEach { (order, partitions) ->
            report.add("$order : 파티션 ${partitions.sorted()} 사용 (${partitions.size}개)")
        }
        report.add("파티션 분포 : ${keyed.distribution.toSortedMap()}")
        val scatteredKeyed = keyed.partitionsByOrder.count { it.value.size > 1 }
        report.add("→ 한 주문의 이벤트가 여러 파티션에 흩어진 경우: $scatteredKeyed / ${ORDERS.size}건")

        report.section("정리 — 가설이 반만 맞았다")
        report.add("Kafka 의 순서 보장 단위가 토픽이 아니라 파티션이라는 것은 맞다")
        report.add("하지만 '키가 없으면 흩어진다'는 항상 참이 아니다")
        report.add("Kafka 2.4+ 의 sticky partitioner 는 batch.size 만큼 쌓일 때까지 한 파티션에 몰아 보낸다")
        report.add("그래서 소량 발행에서는 키가 없어도 한 파티션에 들어가 순서가 지켜지는 것처럼 보인다")
        report.add("발행량이 늘어 배치가 넘어가는 순간 파티션이 바뀌고 거기서부터 순서가 깨진다")
        report.add("결론: 키 없이 순서가 지켜지는 건 보장이 아니라 우연이다. 트래픽이 늘면 조용히 깨진다")
        report.add("주의: 키를 줘도 나중에 파티션 수를 늘리면 키→파티션 매핑이 바뀌어 기존 순서 보장이 무너진다")

        return mapOf(
            "experiment" to "3. partition key",
            "partitions" to PARTITIONS,
            "recordBytesApprox" to PAYLOAD_PADDING + 20,
            "smallBatchNoKey" to summary(small),
            "bulkNoKey" to summary(bulk),
            "withKey" to summary(keyed),
            "log" to report.lines,
        )
    }

    private fun summary(outcome: Outcome) = mapOf(
        "topic" to outcome.topic,
        "messages" to outcome.distribution.values.sum(),
        "partitionsPerOrder" to outcome.partitionsByOrder.mapValues { it.value.sorted() },
        "distribution" to outcome.distribution.toSortedMap(),
        "ordersScattered" to outcome.partitionsByOrder.count { it.value.size > 1 },
    )

    /**
     * 비동기로 모아 보낸 뒤 메타데이터를 확인한다.
     * 건건이 get() 으로 기다리면 배치가 형성되지 않아 sticky 동작을 관찰할 수 없다.
     */
    private fun publish(useKey: Boolean, eventsPerOrder: Int): Outcome {
        val base = if (useKey) "exp3-with-key" else "exp3-no-key"
        val topic = kafka.freshTopic("$base-$eventsPerOrder", PARTITIONS)
        val padding = "x".repeat(PAYLOAD_PADDING)

        val futures = mutableListOf<Pair<String, CompletableFuture<SendResult<String, String>>>>()
        repeat(eventsPerOrder) { i ->
            ORDERS.forEach { order ->
                val value = "$order/evt-$i|$padding"
                val future =
                    if (useKey) kafkaTemplate.send(topic, order, value)
                    else kafkaTemplate.send(topic, value)
                futures += order to future
            }
        }
        kafkaTemplate.flush()

        val partitionsByOrder = linkedMapOf<String, MutableSet<Int>>()
        ORDERS.forEach { partitionsByOrder[it] = mutableSetOf() }
        val distribution = mutableMapOf<Int, Int>()
        futures.forEach { (order, future) ->
            val partition = future.get().recordMetadata.partition()
            partitionsByOrder.getValue(order) += partition
            distribution.merge(partition, 1, Int::plus)
        }
        return Outcome(topic, partitionsByOrder, distribution)
    }

    private data class Outcome(
        val topic: String,
        val partitionsByOrder: Map<String, Set<Int>>,
        val distribution: Map<Int, Int>,
    )
}
