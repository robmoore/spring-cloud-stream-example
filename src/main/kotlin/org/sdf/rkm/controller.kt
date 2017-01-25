package org.sdf.rkm

import mu.KLogging
import org.joda.time.LocalDate
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam

@Controller
class PatientPullController(val apptGateway: AppointmentGateway) {
    companion object : KLogging()

    @RequestMapping("/pullPatients")
    fun pull(@RequestParam(name = "start", defaultValue = "") start: String): ResponseEntity<Any> {
        val startDate = if (start.isEmpty())
            LocalDate.now().minusWeeks(2)
        else
            LocalDate.parse(start)
        val request = AppointmentsRequest(start = startDate.toString())
        logger.debug { "Received request to pull patients - $request" }
        apptGateway.pull(request)
        return ResponseEntity.ok().build()
    }
}