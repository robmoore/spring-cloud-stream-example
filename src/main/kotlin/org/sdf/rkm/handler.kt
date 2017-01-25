package org.sdf.rkm

import mu.KLogging
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.messaging.MessageChannel
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.messaging.support.MessageBuilder
import org.springframework.stereotype.Service

@Service
@EnableBinding(Appointments::class)
class AppointmentsHandler(val docRequestOut: MessageChannel, val greenway: Greenway) {
    companion object : KLogging()

    @StreamListener("apptRequestIn")
    fun process(appointmentsRequest: AppointmentsRequest) {
        logger.info {
            "Pull appointments request received - start: ${appointmentsRequest.start}, " +
                    "end: ${appointmentsRequest.end}"
        }

        greenway.pullAppointments(appointmentsRequest).parallel().forEach {
            docRequestOut.send(MessageBuilder.withPayload(DocumentRequest(it)).build())
        }
    }
}

@Service
@EnableBinding(ClinicalDoc::class)
class ClinicalDocHandler(val greenway: Greenway) {
    companion object : KLogging()

    @StreamListener("docRequestIn")
    @SendTo("archiveRequestOut")
    fun process(documentRequest: DocumentRequest): String {
        logger.info { "Document request received - sourcePatientId: ${documentRequest.sourcePatientId}" }
        return greenway.pullClinicalDoc(documentRequest)
    }
}

@Service
@EnableBinding(Archive::class, Summary::class)
class ArchiveHandler(val greenway: Greenway) {
    companion object : KLogging()

    @StreamListener("archiveRequestIn")
    @SendTo("summaryRequestOut")
    fun process(doc: String): PatientDemo {
        logger.info { "Archive request received - doc: ${doc.take(160)}" }
        return greenway.transformToDemographics(doc)
    }
}

@Service
@EnableBinding(Summary::class)
class SummaryHandler {
    companion object : KLogging()

    @StreamListener("summaryRequestIn")
    fun process(demo: PatientDemo) {
        logger.info { "Summary request received - demo: ${demo}" }
    }
}