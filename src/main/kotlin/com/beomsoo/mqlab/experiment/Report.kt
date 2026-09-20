package com.beomsoo.mqlab.experiment

import org.slf4j.LoggerFactory

/**
 * 실험 결과를 로그와 HTTP 응답에 동시에 남기기 위한 기록기.
 * README 에 붙일 로그를 그대로 복사할 수 있도록 포맷을 단순하게 유지한다.
 */
class Report(name: String) {
    private val logger = LoggerFactory.getLogger(name)
    val lines: MutableList<String> = mutableListOf()

    fun add(message: String) {
        logger.info(message)
        lines += message
    }

    fun section(title: String) {
        add("")
        add("── $title")
    }
}
