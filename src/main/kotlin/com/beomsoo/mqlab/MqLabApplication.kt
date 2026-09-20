package com.beomsoo.mqlab

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class MqLabApplication

fun main(args: Array<String>) {
    runApplication<MqLabApplication>(*args)
}
