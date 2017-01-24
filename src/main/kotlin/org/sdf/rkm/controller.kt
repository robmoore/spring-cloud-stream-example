package org.sdf.rkm

import mu.KLogging
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseBody
import java.util.*

@Controller
class SampleWorkController(val workUnitGateway: WorkUnitGateway) {
    companion object : KLogging()

    @RequestMapping("/generateWork")
    @ResponseBody
    fun generateWork(@RequestParam("definition") definition: String): WorkUnit {
        val sampleWorkUnit = WorkUnit(UUID.randomUUID().toString(), definition)
        logger.debug { "Received request for work unit: ${sampleWorkUnit.id}, ${sampleWorkUnit.definition}" }
        workUnitGateway.generate(sampleWorkUnit)
        return sampleWorkUnit
    }
}
