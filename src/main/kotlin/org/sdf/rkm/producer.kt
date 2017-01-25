package org.sdf.rkm

import org.springframework.integration.annotation.Gateway
import org.springframework.integration.annotation.MessagingGateway

@MessagingGateway
interface WorkUnitGateway {
    @Gateway(requestChannel = WorkOutput.OUTPUT)
    fun generate(workUnit: WorkUnit)
}

