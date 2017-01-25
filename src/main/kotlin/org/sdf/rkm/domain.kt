package org.sdf.rkm

import org.joda.time.LocalDate
import org.springframework.cloud.stream.annotation.Input
import org.springframework.cloud.stream.annotation.Output
import org.springframework.integration.annotation.Gateway
import org.springframework.integration.annotation.MessagingGateway
import org.springframework.messaging.MessageChannel
import org.springframework.messaging.SubscribableChannel

// send message to pull patient IDs -> takes practice ID and date span? -> outputs patientIDs
// send to store doc step -> take patient ID -> outputs document ID
// send to archive step -> takes doc ID -> outputs archive
// send to aggregate step -> takes archive -> outputs summary

data class AppointmentsRequest(val start: String = LocalDate.now().minusYears(2).toString(),
                               val end: String = LocalDate.now().toString())

data class DocumentRequest(val sourcePatientId: Int)

@MessagingGateway
interface AppointmentGateway {
    @Gateway(requestChannel = "apptRequestOut")
    fun pull(appointmentsRequest: AppointmentsRequest)
}

interface Appointments {
    @Input
    fun apptRequestIn(): SubscribableChannel

    @Output
    fun apptRequestOut(): MessageChannel
}

interface ClinicalDoc {
    @Input
    fun docRequestIn(): SubscribableChannel

    @Output
    fun docRequestOut(): MessageChannel
}

interface Archive {
    @Input
    fun archiveRequestIn(): SubscribableChannel

    @Output
    fun archiveRequestOut(): MessageChannel
}

interface Summary {
    @Input
    fun summaryRequestIn(): SubscribableChannel

    @Output
    fun summaryRequestOut(): MessageChannel
}
