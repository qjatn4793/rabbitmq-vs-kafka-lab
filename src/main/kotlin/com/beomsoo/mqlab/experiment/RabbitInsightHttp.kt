package com.beomsoo.mqlab.experiment

import com.fasterxml.jackson.databind.ObjectMapper
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Base64

/** 클러스터 관리 API(포트 15682) 로 quorum queue 의 리더 노드와 적재량을 읽는다. */
class RabbitInsightHttp(private val host: String, private val port: Int = 15682) {
    private val client = HttpClient.newHttpClient()
    private val mapper = ObjectMapper()
    private val auth = "Basic " + Base64.getEncoder().encodeToString("guest:guest".toByteArray())

    fun queueLeader(queue: String): String = node(queue).path("leader").asText("unknown")

    fun queueMessages(queue: String): Int = node(queue).path("messages").asInt(-1)

    private fun node(queue: String) =
        mapper.readTree(get("/api/queues/%2F/" + URLEncoder.encode(queue, Charsets.UTF_8)))

    private fun get(path: String): String {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://$host:$port$path"))
            .header("Authorization", auth)
            .GET().build()
        return client.send(request, HttpResponse.BodyHandlers.ofString()).body()
    }
}
