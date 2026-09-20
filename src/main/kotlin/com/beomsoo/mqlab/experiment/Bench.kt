package com.beomsoo.mqlab.experiment

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.MessageProperties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import com.rabbitmq.client.ConnectionFactory as RabbitConnectionFactory

/**
 * 성능 실험은 프레임워크 오버헤드를 빼고 비교하려고 두 브로커 모두 native client 를 직접 쓴다.
 * 설정을 코드에 그대로 드러내는 편이 "무슨 조건에서 잰 수치인지" 읽는 사람에게 분명하다.
 */
object Bench {

    /** 같은 조건에서 비교하기 위한 내구성 프로파일 */
    enum class Durability(val label: String) {
        /** 복제/디스크 확인까지 기다린다 */
        DURABLE("내구성 우선"),

        /** 확인을 기다리지 않는다. 유실 가능 */
        FAST("속도 우선"),
    }

    fun kafkaProducer(bootstrapServers: String, durability: Durability): KafkaProducer<String, ByteArray> {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java)
            put(ProducerConfig.LINGER_MS_CONFIG, 5)
            put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024)
            when (durability) {
                Durability.DURABLE -> {
                    put(ProducerConfig.ACKS_CONFIG, "all")
                    put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
                }
                Durability.FAST -> {
                    put(ProducerConfig.ACKS_CONFIG, "1")
                    put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false)
                }
            }
        }
        return KafkaProducer(props)
    }

    fun rabbitConnection(host: String): Connection =
        RabbitConnectionFactory().apply {
            this.host = host
            username = "guest"
            password = "guest"
        }.newConnection()

    /**
     * RabbitMQ 발행.
     * DURABLE = persistent 메시지 + publisher confirms(마지막에 일괄 대기)
     * FAST    = transient 메시지 + confirms 없음
     */
    fun rabbitPublish(
        channel: Channel,
        queue: String,
        payload: ByteArray,
        count: Int,
        durability: Durability,
        confirmTimeoutMs: Long = 120_000,
    ) {
        val props =
            if (durability == Durability.DURABLE) MessageProperties.PERSISTENT_BASIC
            else MessageProperties.BASIC
        repeat(count) { channel.basicPublish("", queue, props, payload) }
        // 메모리 알람이 걸리면 여기서 confirm 이 오지 않는다 (발행자 차단)
        if (durability == Durability.DURABLE) channel.waitForConfirmsOrDie(confirmTimeoutMs)
    }

    /** confirmSelect 는 채널당 한 번만 호출해야 한다. */
    fun enableConfirms(channel: Channel) = channel.confirmSelect()

    fun payload(sizeBytes: Int): ByteArray = ByteArray(sizeBytes) { ('a' + (it % 26)).code.toByte() }

    /** 초당 건수와 MB/s 를 함께 낸다. 메시지 크기가 다르면 건수만으로는 비교가 안 되기 때문이다. */
    fun rate(count: Int, sizeBytes: Int, elapsedMs: Long): Map<String, Any> {
        val seconds = elapsedMs / 1000.0
        val perSecond = if (seconds > 0) count / seconds else 0.0
        val mbPerSecond = perSecond * sizeBytes / (1024 * 1024)
        return linkedMapOf(
            "elapsedMs" to elapsedMs,
            "msgPerSec" to Math.round(perSecond),
            "mbPerSec" to String.format("%.1f", mbPerSecond).toDouble(),
        )
    }

    fun format(name: String, stats: Map<String, Any>): String =
        "%-34s %7s ms   %9s msg/s   %6s MB/s".format(
            name,
            stats["elapsedMs"],
            stats["msgPerSec"],
            stats["mbPerSec"],
        )
}
