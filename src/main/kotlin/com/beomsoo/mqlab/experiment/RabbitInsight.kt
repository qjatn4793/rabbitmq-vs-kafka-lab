package com.beomsoo.mqlab.experiment

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Base64

/**
 * RabbitMQ 관리 API 로 브로커 내부 상태를 읽는다.
 * "느려졌다"가 아니라 "메모리 알람이 걸려 연결이 blocked 되었다"까지 근거로 남기기 위함이다.
 */
@Component
class RabbitInsight(
    @Value("\${spring.rabbitmq.host}") private val host: String,
) {
    private val client: HttpClient = HttpClient.newHttpClient()
    private val mapper = ObjectMapper()
    private val auth = "Basic " + Base64.getEncoder().encodeToString("guest:guest".toByteArray())

    fun nodeStatus(): NodeStatus = runCatching {
        val node = mapper.readTree(get("/api/nodes")).first()
        NodeStatus(
            memoryUsedBytes = node.path("mem_used").asLong(),
            memoryLimitBytes = node.path("mem_limit").asLong(),
            memoryAlarm = node.path("mem_alarm").asBoolean(),
            diskFreeAlarm = node.path("disk_free_alarm").asBoolean(),
        )
    }.getOrElse { NodeStatus(-1, -1, false, false) }

    /** blocked / blocking / running 상태의 연결 수 */
    fun connectionStates(): Map<String, Int> = runCatching {
        mapper.readTree(get("/api/connections"))
            .map { it.path("state").asText("unknown") }
            .groupingBy { it }
            .eachCount()
    }.getOrElse { emptyMap() }

    private fun get(path: String): String {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://$host:15672$path"))
            .header("Authorization", auth)
            .GET()
            .build()
        return client.send(request, HttpResponse.BodyHandlers.ofString()).body()
    }

    data class NodeStatus(
        val memoryUsedBytes: Long,
        val memoryLimitBytes: Long,
        val memoryAlarm: Boolean,
        val diskFreeAlarm: Boolean,
    ) {
        fun describe(): String =
            "메모리 %.0fMB 사용 / 임계치 %.0fMB, memory alarm=%s".format(
                memoryUsedBytes / 1024.0 / 1024,
                memoryLimitBytes / 1024.0 / 1024,
                memoryAlarm,
            )
    }
}
