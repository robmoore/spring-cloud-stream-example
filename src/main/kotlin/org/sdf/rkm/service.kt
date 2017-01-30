package org.sdf.rkm

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.kittinunf.fuel.httpPost
import com.google.common.util.concurrent.RateLimiter
import com.google.i18n.phonenumbers.PhoneNumberUtil
import com.natpryce.konfig.*
import kotlinx.support.jdk8.collections.stream
import mu.KLogging
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Days
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.openhealthtools.mdht.uml.cda.util.CDAUtil
import org.springframework.stereotype.Service
import java.util.concurrent.TimeUnit
import java.util.stream.Stream

private val YMD_FORMAT = DateTimeFormat.forPattern("yyyyMMdd")

val PAGE_SIZE = 10

@Service
class Greenway(val config: Configuration = ConfigurationProperties.fromResource("default.properties")) {
    companion object : KLogging()

    // format: greenway.serviceUsername, greenway.servicePassword, etc
    object greenway : PropertyGroup() {
        val apiKey by stringType
        val vendorLogin by stringType
        val vendorPassword by stringType
        val uri by stringType
        val primeSuiteSiteId by stringType
        val primeSuiteUserId by intType
        val primeSuiteUserName by stringType
        val primeSuiteUserPassword by stringType
    }

    val objectMapper: ObjectMapper = jacksonObjectMapper()
    val throttle: RateLimiter = RateLimiter.create(5.0, 5L, TimeUnit.SECONDS)
    val readTimeout = 120000

    init {
        CDAUtil.loadPackages()
        objectMapper.registerModule(JodaModule())
        objectMapper.propertyNamingStrategy = PropertyNamingStrategy.UPPER_CAMEL_CASE
    }

    fun pullAppointments(appointmentsRequest: AppointmentsRequest): Stream<Int> {
        val weeks = splitIntoWeeks(appointmentsRequest.start, appointmentsRequest.end)

        val patientList = weeks.parallel().flatMap {
            getPatientList(it.first, it.second)
        }

        return patientList.map { it.patient.patientID }.filter { it != 0 }.distinct()
    }

    fun pullClinicalDoc(documentRequest: DocumentRequest): String {
        return getClinicalSummary(documentRequest.sourcePatientId)
    }

    fun transformToDemographics(clinicalDoc: String): PatientDemo {
        val clinicalDocument = try {
            CDAUtil.load(clinicalDoc.byteInputStream())
        } catch (e: ClassCastException) {
            logger.error { "Unable to process CDA for input" }
            throw e
        }
        val patientRole = clinicalDocument.recordTargets.first().patientRole
        val mrn = patientRole.ids.first().extension

        val firstName = patientRole.patient.names.first().givens.first().text
        val lastName = patientRole.patient.names.first().families.first().text
        val birthTime = patientRole.patient.birthTime.value
        val dateOfBirth: String = if (birthTime != null) {
            YMD_FORMAT.parseLocalDate(birthTime).toString()
        } else ""
        val gender = patientRole.patient.administrativeGenderCode.code


        val addr = patientRole.addrs.first()
        val city = addr.cities.first().text
        val state = addr.states.first().text
        val zip = addr.postalCodes.first().text
        val streetAddress = addr.streetAddressLines.first().text

        val telecom = patientRole.telecoms.first()

        val phoneUtil = PhoneNumberUtil.getInstance()
        val telecomValue = telecom.value
        val phone = if (telecomValue != null && !telecomValue.isEmpty()) {
            val formattedPhone = phoneUtil.parse(telecomValue, "US")
            phoneUtil.format(formattedPhone, PhoneNumberUtil.PhoneNumberFormat.NATIONAL)
        } else ""

        val author = clinicalDocument.authors.first()
        val assignedProviderName = author.assignedAuthor.assignedPerson.names.first()
        val assignedProviderFirstName = assignedProviderName.givens.first().text
        val assignedProviderLastName = assignedProviderName.families.first().text

        return PatientDemo(mrn, firstName, lastName, dateOfBirth, gender, phone, streetAddress, city, state, zip,
                assignedProviderFirstName, assignedProviderLastName)
    }

    fun getPatientList(fromDate: LocalDate, toDate: LocalDate): Stream<Appointment> {
        // We need to shave off the last one here to avoid duplicate entry
        val requestBody = makePatientListRequest(fromDate, toDate)
        logger.debug { "Request: $requestBody" }
        throttle.acquire()

        val (request, response, result) = "${config[greenway.uri]}/User/PatientListGet?api_key=${config[greenway.apiKey]}".httpPost().
                timeoutRead(readTimeout).body(requestBody).
                header("Content-Type" to "application/json").
                responseString()

        logger.debug(request.toString())

        val responseBody = result.get()
        logger.debug { "Response: $responseBody" }
        val patientList = objectMapper.readValue(responseBody, PatientListResponse::class.java)

        logger.debug { "Total rows for $fromDate to $toDate: ${patientList.totalRows} [${patientList.totalRows / PAGE_SIZE} pages]" }

        return if (PAGE_SIZE < patientList.totalRows) {
            val lastPage = patientList.totalRows / PAGE_SIZE
            val otherPages = (1..lastPage)
                    .map {
                        makePatientListRequest(fromDate, toDate, pageStart = it * PAGE_SIZE + 1)
                    }
                    .map {
                        logger.debug { "Request: $it" }
                        throttle.acquire()
                        "${config[greenway.uri]}/User/PatientListGet?api_key=${config[greenway.apiKey]}".httpPost().
                                timeoutRead(readTimeout).
                                body(it).
                                header("Content-Type" to "application/json").
                                responseString()
                    }
                    .map {
                        logger.debug { it.first.toString() }
                        it.third.get()
                    }
                    .map {
                        logger.debug { "Response: $it" }
                        objectMapper.readValue(it, PatientListResponse::class.java)
                    }
                    .flatMap { it.appointments }
            val combinedAppointments = patientList.appointments + otherPages
            combinedAppointments.stream()
        } else {
            patientList.appointments.stream()
        }
    }

    private fun makePatientListRequest(fromDate: LocalDate, toDate: LocalDate, pageStart: Int = 0, pageSize: Int = PAGE_SIZE): String {
        val requestBody = objectMapper.writeValueAsString(PatientListRequest(credentials = makeCredentials(),
                header = makeHeader(), fromDate = fromDate.toDateTimeAtStartOfDay().withZone(DateTimeZone.UTC),
                toDate = toDate.toDateTimeAtCurrentTime().millisOfDay().withMaximumValue().withZone(DateTimeZone.UTC),
                pageStart = pageStart, pageSize = pageSize))
        return requestBody
    }

    fun getClinicalSummary(patientId: Int): String {
        val requestBody = objectMapper.writeValueAsString(ClinicalSummaryRequest(credentials = makeCredentials(),
                header = makeHeader(), patientID = patientId))
        logger.debug { "Request: $requestBody" }
        throttle.acquire()
        val (request, response, result) = "${config[greenway.uri]}/Patient/ClinicalSummaryGet?api_key=${config[greenway.apiKey]}".httpPost().
                timeoutRead(readTimeout).
                body(requestBody).
                header("Content-Type" to "application/json").
                responseString()
        val responseBody = result.get()
        logger.debug { "Response: $responseBody" }
        val clinicalSummary = objectMapper.readValue(result.get(), ClinicalSummaryResponse::class.java)
        return clinicalSummary.data
    }

    fun makeCredentials(): Credentials = // primeSuiteSiteId, primeSuiteUserName, primeSuiteUserPassword are from practice
            Credentials(PrimeSuiteCredential(primeSuiteSiteId = config[greenway.primeSuiteSiteId],
                    primeSuiteUserName = config[greenway.primeSuiteUserName],
                    primeSuiteUserPassword = config[greenway.primeSuiteUserPassword]),
                    VendorCredential(vendorLogin = config[greenway.vendorLogin],
                            vendorPassword = config[greenway.vendorPassword]))

    fun makeHeader(): Header = Header(destinationSiteID = config[greenway.primeSuiteSiteId],
            primeSuiteUserID = config[greenway.primeSuiteUserId])

    fun splitIntoWeeks(startDate: LocalDate, endDate: LocalDate): Stream<Pair<LocalDate, LocalDate>> {
        val nDays = Days.daysBetween(startDate, endDate).days + 1 /* since between */
        val nWeeks = nDays / 7

        val weeks = arrayListOf<Pair<LocalDate, LocalDate>>()
        if (nDays > 7) {
            var startOfWeek = startDate
            var endOfWeek = startOfWeek.plusDays(6)
            for (i in 0..nWeeks - 1) {
                val week = Pair(startOfWeek, endOfWeek)
                weeks.add(week)
                startOfWeek = startOfWeek.plusWeeks(1)
                endOfWeek = endOfWeek.plusWeeks(1)
            }
            // add left over days
            if (nDays % 7 != 0) {
                val week = Pair(startOfWeek, endDate)
                weeks.add(week)
            }
        } else {
            weeks.add(Pair(startDate, endDate))
        }

        return weeks.stream()
    }
}

@JsonPropertyOrder("mrn", "firstName", "lastName", "gender", "dateOfBirth", "phone", "streetAddress", "city",
        "state", "zip", "assignedProviderFirstName", "assignedProviderLastName")
data class PatientDemo(val mrn: String, val firstName: String, val lastName: String, val dateOfBirth: String, val gender: String,
                       val phone: String, val streetAddress: String, val city: String, val state: String, val zip: String, val assignedProviderFirstName: String,
                       val assignedProviderLastName: String)

data class ClinicalSummaryResponse(val data: String /* xml */, val extractDateTime: String)

data class Credentials(val primeSuiteCredential: PrimeSuiteCredential, val vendorCredential: VendorCredential)
data class PrimeSuiteCredential(val primeSuiteSiteId: String, val primeSuiteUserAlias: String = "",
                                val primeSuiteUserName: String, val primeSuiteUserPassword: String)

data class VendorCredential(val vendorLogin: String, val vendorPassword: String)
data class Header(val destinationSiteID: String, val primeSuiteUserID: Int, val sourceSiteID: String = "")

class MsJsonDateSerializer : JsonSerializer<DateTime>() {
    val dateTimeFormatter: DateTimeFormatter = DateTimeFormat.forPattern("Z")

    override fun serialize(value: DateTime, gen: JsonGenerator, serializers: SerializerProvider) {
        gen.writeString("""/Date(${value.millis}${dateTimeFormatter.withZone(value.zone).print(0)})/""")
    }
}

class MsJsonDateDeserializer : JsonDeserializer<DateTime>() {
    val pattern = """\\?\/Date\((-)?(\d+)([-+]\d+)?\)\/\\?""".toPattern()

    override fun deserialize(jp: JsonParser, ctx: DeserializationContext): DateTime? {
        val dateString = jp.text
        val matcher = pattern.matcher(dateString)
        return if (matcher.find()) {
            if (matcher.groupCount() == 3) {
                // 1 == (-)
                val time = matcher.group(2)
                val offset = matcher.group(3)
                val timezone = if (offset != null) {
                    DateTimeZone.forID(offset)
                } else {
                    DateTimeZone.forOffsetHours(0)
                }
                DateTime(-time.toLong(), timezone)
            } else {
                val time = matcher.group(1)
                val offset = matcher.group(2)
                val timezone = if (offset != null) {
                    DateTimeZone.forID(offset)
                } else {
                    DateTimeZone.forOffsetHours(0)
                }
                DateTime(time.toLong(), timezone)
            }
        } else throw RuntimeException("Couldn't parse date: $dateString")
    }
}

data class PatientListRequest(val credentials: Credentials,
                              val header: Header,
                              val appointmentTypeIDs: List<Int> = listOf(0),
                              val careProviderIDs: List<Int> = listOf(0),
                              val defaultFilterID: Int = 0,
                              @JsonSerialize(using = MsJsonDateSerializer::class) val fromDate: DateTime,
                              val pageSize: Int = 10,
                              val pageStart: Int = 0,
                              val serviceLocationIDs: List<Int> = listOf(0),
                              @JsonSerialize(using = MsJsonDateSerializer::class) val toDate: DateTime,
                              val visitTypeIDs: List<Int> = listOf(0))

data class PatientListResponse(val appointments: List<Appointment>, val totalRows: Int)

data class CDAProfile(val profileType: Int = 0, val visitId: List<Int> = listOf(0))
data class ClinicalSummaryRequest(@JsonProperty("CDADocumentType") val cdaDocumentType: Int = 1001,
                                  @JsonProperty("CDAProfile") val cdaProfile: CDAProfile = CDAProfile(),
                                  val credentials: Credentials,
                                  val header: Header,
                                  val patientID: Int)

data class Insurance(val address: String?, val companyName: String?, val effectiveFrom: String?, val effectiveTo: String?,
                     val eligibilityStatus: String?, val insDocVisitCoPay: Int, val insuranceID: String?,
                     val insuranceName: String?, val planID: String?, val planName: String?, val planNumber: String?)

data class Location(val abbr: String?, val address: String?, val appointmentCat: Int, val appointmentCategories: String?,
                    val appointmentType: Int, val locationID: Int, val name: String?, val resourceCat: Int, val resourceID: Int)

data class Patient(val accountBalanceCalcMethod: Int,
                   val alternatePatientID: Int,
                   val assignmentOfBenifits: Int,
                   val cellPhoneNumber1: String?,
                   val cellPhoneNumber2: String?,
                   val citizenship: Int,
                   val credential: Int,
                   val customCredential: String?,
                   @JsonDeserialize(using = MsJsonDateDeserializer::class) val dateOfBirth: DateTime?,
                   val deceased: Int,
                   @JsonDeserialize(using = MsJsonDateDeserializer::class) val deceasedDate: DateTime?,
                   val doesPatientHaveResidentProof: Boolean,
                   val driversLicenseNumber: String?,
                   val driversLicenseState: Int,
                   val emailAddress1: String?,
                   val emailAddress2: String?,
                   val enabled: Boolean,
                   val ethnicity: Int,
                   val firstname: String?,
                   val fullName: String,
                   val gender: Int,
                   val guarantorID: Int,
                   val homePhoneNumber: String?,
                   val isFinancialRisk: Boolean,
                   val isPatient: Boolean,
                   val languageLookUpID: String?,
                   val lastName: String?,
                   val localName: String?,
                   val maidenName: String?,
                   val maritalStatus: Int,
                   val middleName1: String?,
                   val middleName2: String?,
                   val mothersMaidenName: String?,
                   val nickname: String,
                   val noteText: String?,
                   val notes: String?,
                   val occupation: String?,
                   val originalPatientID: Int,
                   val originalPatientIDString: String?,
                   val pagerNumber: String?,
                   val personID: Int,
                   val pharmacyName: String?,
                   val pharmacyNoteID: Int,
                   val phoneNumber1: String?,
                   val phoneNumber2: String?,
                   val picture: String?,
                   val preferredCommunicationsID: Int,
                   val preferredPharmacyID: Int,
                   val preferredProvider: String?,
                   val preferredProviderID: Int,
                   val preferredServiceLoc: String?,
                   val preferredServiceLocationID: Int,
                   val prefix: Int,
                   val primaryCareProvider: String?,
                   val primaryLanguage: Int,
                   val primaryPhoneNumber: String?,
                   val primaryPhysID: Int,
                   val primaryWorkPhone: String?,
                   val race: Int,
                   val referringProvider: String?,
                   val releaseOfInformation: Boolean,
                   val religion: Int,
                   val residentialAddress: String?,
                   val SSN: String?,
                   val sealedFlagID: Int,
                   val secondaryLanguage: Int,
                   val secondaryRaces: String?,
                   val studentStatusID: Int,
                   val suffix: Int,
                   val patientID: Int,
                   val photoPath: String?,
                   val PBMConsent: Int,
                   val otherID: String?,
                   val patientAge: String?,
                   val patientStatusID: Int)

data class Resource(val altLabel: String?,
                    val careProviderName: String?,
                    val description: String?,
                    val name: String,
                    val overloadLimit: Int,
                    val ownerCareProviderID: Int,
                    val preferredDuration: Int,
                    val resourceID: Int)

data class Tracking(val area: String?,
                    val areaID: Int,
                    val changedByTime: String?,
                    val responsible: String?,
                    val responsibleID: Int,
                    val status: String?,
                    val statusID: Int,
                    val timeIn: String?,
                    val timeOut: String?,
                    val trackingID: Int,
                    val trackingUser: String?)

data class Type(val abbr: String?,
                val altLabel: String?,
                val appointmentTypeID: Int,
                val duration: Int,
                val name: String,
                val patientInstructions: String?,
                val resourceCategories: String?)

data class Document(val clinicalDocumentID: String,
                    val clinicalDocumentName: String,
                    val clinicalDocumentStatus: String,
                    val clinicalDocumentType: String,
                    val clinicalDocumentTypeID: String)

data class ServiceDetail(val balanceCalcMethod: String?,
                         val batchID: String?,
                         val billableToPatient: Boolean,
                         val careProviderID: Int,
                         val charge: String?,
                         val chargeAmount: String?,
                         val chargeBatchID: Int,
                         val chargeCreateDate: String?,
                         val chargeID: Int,
                         val chargePostingDate: String?,
                         val chargeType: String?,
                         val coPayAmtApplied: String?,
                         val diag1: String?,
                         val diag2: String?,
                         val diag3: String?,
                         val diag4: String?,
                         val fromDate: String?,
                         val ICD10Diag1: String?,
                         val ICD10Diag2: String?,
                         val ICD10Diag3: String?,
                         val ICD10Diag4: String?,
                         val locationID: Int,
                         val NPI: String,
                         val nonCoveredCharge: String?,
                         val numberOfDaysOrUnits: String?,
                         val placeOfServiceID: String?,
                         val postingDate: String?,
                         val practiceLocationID: Int,
                         val procedureAmount: String?,
                         val procedureCode: String?,
                         val referringProviderID: Int,
                         val renderingProviderID: Int,
                         val renderingProviderNPI: String?,
                         val rxNumber: String?,
                         val serviceDetailDiagnosis: List<Map<String, String>>?,
                         val serviceDetailID: Int,
                         val toDate: String?,
                         val typeOfService: String?)

data class Visit(val accidentFlag: String?,
                 val accidentState: String?,
                 val accidentTypeID: Int,
                 val billableCareProviderID: Int,
                 val billableCareProviderNPI: String?,
                 val billableCareProviderName: String?,
                 val careProviderDirectEmailAddress: String?,
                 val careProviderID: Int,
                 val careProviderName: String?,
                 val chargeTicketID: Int,
                 val comments: String?,
                 val createDate: String?,
                 val createUserID: Int,
                 val disabilityFromDate: String?,
                 val disabilityToDate: String?,
                 val documents: List<Document>?,
                 val financiallyResponsiblePartyID: Int,
                 @JsonDeserialize(using = MsJsonDateDeserializer::class) val fromDateTime: DateTime,
                 val hospitalAdmitDateTime: String?,
                 val hospitalDischargeDateTime: String?,
                 val lastChangeUserID: Int,
                 val patArrDateTime: String?,
                 val patientArrivalFlag: String?,
                 val patientID: Int,
                 val practiceLocationID: Int,
                 val primaryCareProviderID: Int,
                 val primaryComplaint: String?,
                 val primaryDiagnosis: String?,
                 val quality: String?,
                 val referringProviderID: Int,
                 val referringProviderNPI: String?,
                 val referringProviderName: String?,
                 val releasedVisitInfoFlag: Int,
                 val serviceDetail: List<ServiceDetail>?,
                 val serviceLocationID: Int,
                 val similarIllnessDate: String?,
                 val superBillStatus: String?,
                 val superbillID: Int,
                 val symptomDate: String?,
                 val throughDateTime: String?,
                 val visitID: Int,
                 val visitStatus: String?,
                 val visitTypeID: Int,
                 val visitTypeName: String,
                 @JsonProperty("sChargeTicketID") val sChargeTicketID: String?,
                 val isCheckedOut: Boolean)

data class Appointment(val appointmentID: Int, val appointmentTime: String, val category: String?, val chargesEntered: Int,
                       val chiefComplaint: String, val comments: String?, val copayPosted: String, val endTime: String?,
                       val insurance: Insurance, val location: Location, val options: Int, val patient: Patient,
                       val picture: Boolean, val RTC: String, val resource: Resource,
                       @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "MM/dd/yyyy") val scheduleDate: LocalDate?,
                       val startTime: String?, val tracking: Tracking, val type: Type, val visit: Visit)