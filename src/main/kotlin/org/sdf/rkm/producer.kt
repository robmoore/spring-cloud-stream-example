package org.sdf.rkm

import org.springframework.cloud.stream.messaging.Source
import org.springframework.integration.annotation.Gateway
import org.springframework.integration.annotation.MessagingGateway

@MessagingGateway
interface WorkUnitGateway {
    @Gateway(requestChannel = Source.OUTPUT)
    fun generate(workUnit: WorkUnit)
}

