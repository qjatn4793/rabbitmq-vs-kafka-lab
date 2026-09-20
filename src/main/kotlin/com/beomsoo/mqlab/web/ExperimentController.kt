package com.beomsoo.mqlab.web

import com.beomsoo.mqlab.experiment.BacklogExperiment
import com.beomsoo.mqlab.experiment.BlockingExperiment
import com.beomsoo.mqlab.experiment.FailoverExperiment
import com.beomsoo.mqlab.experiment.OrderingExperiment
import com.beomsoo.mqlab.experiment.PartitionKeyExperiment
import com.beomsoo.mqlab.experiment.ReplayExperiment
import com.beomsoo.mqlab.experiment.ThroughputExperiment
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/experiments")
class ExperimentController(
    private val replay: ReplayExperiment,
    private val ordering: OrderingExperiment,
    private val partitionKey: PartitionKeyExperiment,
    private val blocking: BlockingExperiment,
    private val throughput: ThroughputExperiment,
    private val backlog: BacklogExperiment,
    private val failover: FailoverExperiment,
) {
    @GetMapping("/1")
    fun replay(@RequestParam(defaultValue = "10") count: Int) = replay.run(count)

    @GetMapping("/2")
    fun ordering(
        @RequestParam(defaultValue = "30") count: Int,
        @RequestParam(defaultValue = "3") consumers: Int,
    ) = ordering.run(count, consumers)

    @GetMapping("/3")
    fun partitionKey() = partitionKey.run()

    @GetMapping("/4")
    fun blocking(@RequestParam(defaultValue = "10") count: Int) = blocking.run(count)

    @GetMapping("/5")
    fun throughput(
        @RequestParam(defaultValue = "100000") count: Int,
        @RequestParam(defaultValue = "512") sizeBytes: Int,
    ) = throughput.run(count, sizeBytes)

    @GetMapping("/6")
    fun backlog(
        @RequestParam(defaultValue = "20") chunks: Int,
        @RequestParam(defaultValue = "10000") chunkSize: Int,
    ) = backlog.run(chunks, chunkSize)

    // 실험 7 은 발행 도중 밖에서 노드를 죽여야 하므로 start / result 를 나눈다
    @GetMapping("/7/kafka/start")
    fun kafkaFailoverStart(
        @RequestParam(defaultValue = "45") durationSec: Int,
        @RequestParam(defaultValue = "20") intervalMs: Long,
    ) = failover.startKafka(durationSec, intervalMs)

    @GetMapping("/7/kafka/result")
    fun kafkaFailoverResult() = failover.result("kafka")

    @GetMapping("/7/rabbit/start")
    fun rabbitFailoverStart(
        @RequestParam(defaultValue = "45") durationSec: Int,
        @RequestParam(defaultValue = "20") intervalMs: Long,
    ) = failover.startRabbit(durationSec, intervalMs)

    @GetMapping("/7/rabbit/result")
    fun rabbitFailoverResult() = failover.result("rabbit")
}
