package org.sdf.rkm

import org.springframework.cloud.stream.annotation.Input
import org.springframework.cloud.stream.annotation.Output
import org.springframework.messaging.MessageChannel
import org.springframework.messaging.SubscribableChannel


data class WorkUnit(val id: String, val definition: String)

interface WorkOutput {
    companion object {
        const val OUTPUT = "workOutput"
    }

    @Output(OUTPUT)
    fun output(): MessageChannel
}

interface WorkInput {
    companion object {
        const val INPUT = "workInput"
    }

    @Input(INPUT)
    fun input(): SubscribableChannel
}
