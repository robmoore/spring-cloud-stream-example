package org.sdf.rkm

import mu.KLogging
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Sink
import org.springframework.stereotype.Service

@Service
class WorkHandler {
    companion object : KLogging()

    @StreamListener(Sink.INPUT)
    fun process(workUnit: WorkUnit) {
        logger.info { "Handling work unit - id: ${workUnit.id}, definition: ${workUnit.definition}" }
    }
}

