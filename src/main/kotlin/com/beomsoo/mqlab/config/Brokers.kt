package com.beomsoo.mqlab.config

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.DirectExchange
import org.springframework.amqp.core.Queue
import org.springframework.amqp.core.QueueBuilder
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.core.RabbitAdmin
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import java.util.Properties

/**
 * 실험마다 토픽/큐를 새로 만들어 매번 같은 조건에서 시작하도록 돕는다.
 * 이전 실행에서 남은 메시지가 결과를 오염시키지 않게 하는 것이 목적이다.
 */
@Component
class KafkaSupport(
    @Value("\${spring.kafka.bootstrap-servers}") val bootstrapServers: String,
) {
    /**
     * 실행마다 새 이름의 토픽을 만든다.
     *
     * 같은 이름을 지웠다 다시 만들면 멱등 프로듀서(enable.idempotence, Kafka 3.0+ 기본값)가
     * 들고 있던 시퀀스 번호와 브로커 상태가 어긋나 OUT_OF_ORDER_SEQUENCE_NUMBER 가
     * 무한 재시도된다. 이름을 새로 쓰면 그 문제를 피할 수 있다.
     */
    fun freshTopic(baseName: String, partitions: Int): String {
        val topic = "$baseName-${System.currentTimeMillis()}"
        admin().use { admin ->
            // 이전 실행에서 남은 토픽은 정리한다. 다시 produce 하지 않으므로 안전하다.
            val stale = admin.listTopics().names().get().filter { it.startsWith("$baseName-") }
            if (stale.isNotEmpty()) admin.deleteTopics(stale).all().get()
            admin.createTopics(listOf(NewTopic(topic, partitions, 1))).all().get()
            awaitUntil { topic in admin.listTopics().names().get() }
        }
        return topic
    }

    /** 실험마다 새 그룹을 써서 이전 실행의 커밋된 오프셋에 영향을 받지 않게 한다. */
    fun newConsumer(groupId: String): KafkaConsumer<String, String> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100)
        }
        return KafkaConsumer(props)
    }

    private fun admin(): Admin =
        Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers))

    private fun awaitUntil(timeoutMs: Long = 10_000, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return
            Thread.sleep(100)
        }
        error("조건이 ${timeoutMs}ms 안에 만족되지 않았습니다")
    }
}

/**
 * Spring Boot 는 amqpAdmin 을 AmqpAdmin 타입으로만 노출하므로
 * getQueueInfo() 같은 RabbitAdmin 전용 API 를 쓰려면 직접 빈으로 선언해야 한다.
 */
@Configuration
class RabbitAdminConfig {
    @Bean
    fun rabbitAdmin(connectionFactory: ConnectionFactory): RabbitAdmin = RabbitAdmin(connectionFactory)
}

@Component
class RabbitSupport(private val rabbitAdmin: RabbitAdmin) {

    fun recreateQueue(name: String) {
        rabbitAdmin.deleteQueue(name)
        rabbitAdmin.declareQueue(Queue(name, true, false, false))
    }

    /** 메인 큐 + dead letter 전용 큐를 한 쌍으로 만든다. */
    fun recreateQueueWithDeadLetter(name: String, deadLetterQueue: String) {
        val exchange = "$name.dlx"
        rabbitAdmin.deleteQueue(name)
        rabbitAdmin.deleteQueue(deadLetterQueue)
        rabbitAdmin.deleteExchange(exchange)

        rabbitAdmin.declareExchange(DirectExchange(exchange, true, false))
        rabbitAdmin.declareQueue(Queue(deadLetterQueue, true, false, false))
        rabbitAdmin.declareBinding(
            BindingBuilder.bind(Queue(deadLetterQueue, true, false, false))
                .to(DirectExchange(exchange, true, false))
                .with(deadLetterQueue),
        )
        rabbitAdmin.declareQueue(
            QueueBuilder.durable(name)
                .deadLetterExchange(exchange)
                .deadLetterRoutingKey(deadLetterQueue)
                .build(),
        )
    }

    fun messageCount(queue: String): Int =
        rabbitAdmin.getQueueInfo(queue)?.messageCount ?: 0
}
