package no.nb.nifi.tekst.mets

import no.nb.nifi.tekst.exception.MetsCreateException
import org.w3c.dom.Document
import org.w3c.dom.NodeList
import java.io.File
import java.io.IOException
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.math.pow

class JhoveParser(file: File) {
    private var document: Document? = null
    var fileSuffix: String
    private val okStatus = "Well-Formed"

    init {
        val parts = file.name.split(".")
        fileSuffix = parts[parts.size - 2] // TODO? check if we can just hard code this for METS-browsing.xml (jp2)

        try {
            document = XmlHelper.loadDocument(file, false)
        } catch (e: IOException) {
            throw MetsCreateException("Error reading jhove file " + e.message)
        }
    }

    // TODO do we need to set getters?
    val imageWidth: BigInteger = getValueAsBigInt("//BasicImageInformation/BasicImageCharacteristics/imageWidth") ?: throw MetsCreateException("Unable to get image width")

    val imageHeigth: BigInteger = getValueAsBigInt("//BasicImageInformation/BasicImageCharacteristics/imageHeight") ?: throw MetsCreateException("Unable to get image height")

    @Throws(MetsCreateException::class)
    fun samplingFrequencyUnit(): BigInteger? =
        when (fileSuffix) {
            "tif", "tiff", "jpg" -> getValueAsBigInt("//ImageAssessmentMetadata/SpatialMetrics/samplingFrequencyUnit")
            "jp2" -> 3.0.toInt().toBigInteger()
            else -> null
        }

    @Throws(MetsCreateException::class)
    fun xDenominator(): BigInteger? =
        when (fileSuffix) {
            "tif", "tiff", "jpg" -> getValueAsBigInt("//ImageAssessmentMetadata/SpatialMetrics/xSamplingFrequency/denominator")
            "jp2" -> getValueAsBigInt("/jhove/repInfo/properties/property/values/property[./name='DefaultDisplayResolution']/values/property[./name='HorizResolution']/values/property[./name='Denominator']/values/value")
            else -> null
        }

    @Throws(MetsCreateException::class)
    fun yDenominator(): BigInteger? =
        when (fileSuffix) {
            "tif", "tiff", "jpg" -> getValueAsBigInt("//ImageAssessmentMetadata/SpatialMetrics/ySamplingFrequency/denominator")
            "jp2" -> getValueAsBigInt("//jhove/repInfo/properties/property/values/property[./name='DefaultDisplayResolution']/values/property[./name='VertResolution']/values/property[./name='Denominator']/values/value")
            else -> null
        }

    @Throws(MetsCreateException::class)
    fun xNumerator(): BigDecimal? =
        when (fileSuffix) {
            "tif", "tiff", "jpg" -> getValueAsInt("//ImageAssessmentMetadata/SpatialMetrics/xSamplingFrequency/numerator")?.toBigDecimal()
            "jp2" -> {
                val exponent =
                    getValueAsInt("/jhove/repInfo/properties/property/values/property[./name='DefaultDisplayResolution']/values/property[./name='HorizResolution']/values/property[./name='Exponent']/values/value")!!
                val horizRes =
                    getValueAsInt("/jhove/repInfo/properties/property/values/property[./name='DefaultDisplayResolution']/values/property[./name='HorizResolution']/values/property[./name='Numerator']/values/value")!!
                // JPEG 2000 oppgir oppløsning i punkt/m. Vi må konvertere til punkt/cm for å være innenfor MIX-standarden
                (horizRes * 10.0.pow(exponent.toDouble()) / 100).toBigDecimal()
            }
            else -> null
        }


    @Throws(MetsCreateException::class)
    fun yNumerator(): BigDecimal? =
        when (fileSuffix) {
            "tif", "tiff", "jpg" -> getValueAsBigInt("//ImageAssessmentMetadata/SpatialMetrics/ySamplingFrequency/numerator")?.toBigDecimal()
            "jp2" -> {
                val exponent =
                    getValueAsInt("/jhove/repInfo/properties/property/values/property[./name='DefaultDisplayResolution']/values/property[./name='VertResolution']/values/property[./name='Exponent']/values/value")!!
                val vertRes =
                    getValueAsInt("/jhove/repInfo/properties/property/values/property[./name='DefaultDisplayResolution']/values/property[./name='VertResolution']/values/property[./name='Numerator']/values/value")!!
                // JPEG 2000 oppgir oppløsning i punkt/m. Vi må konvertere til punkt/cm for å være innenfor MIX-standarden
                (vertRes * 10.0.pow(exponent) / 100).toBigDecimal()
            }
            else -> null
        }

    val dateLastModified: String = getValueAsString("/jhove/repInfo/lastModified") ?: throw MetsCreateException("Unable to get date last modified")

    @get:Throws(MetsCreateException::class)
    val date: String = getValueAsString("/jhove/date") ?: throw MetsCreateException("Unable to get date")

    @get:Throws(MetsCreateException::class)
    val size: Long?
        get() = getValueAsLong("/jhove/repInfo/size")

    @get:Throws(MetsCreateException::class)
    val formatName: String?
        get() = getValueAsString("/jhove/repInfo/format")

    @get:Throws(MetsCreateException::class)
    val formatVersion: String?
        get() = getValueAsString("/jhove/repInfo/version")

    @get:Throws(MetsCreateException::class)
    val mD5Checksum: String?
        get() = getValueAsString("/jhove/repInfo/checksums/checksum[attribute::type='MD5']")

    @get:Throws(MetsCreateException::class)
    val sHA1Checksum: String?
        get() = getValueAsString("/jhove/repInfo/checksums/checksum[attribute::type='SHA-1']")

    val creatingApplication: String?
        get() {
            return when (fileSuffix) {
                "jp2" -> getValueAsString(
                    "/jhove/repInfo/properties/property/values/property/values/property/values/property[./name='Comments']/" +
                            "values/property[./name='Comment'][1]/values/value"
                )
                "tif" -> getValueAsString(
                    "/jhove/repInfo/properties/property/values/property/values/property/values/property[./name='Entries']/values/property" +
                            "[./name='NisoImageMetadata']/values/value/mix/ImageCaptureMetadata/ScannerCapture/ScanningSystemSoftware/scanningSoftwareName"
                )
                else -> null
            }
        }

    @get:Throws(MetsCreateException::class)
    val status: String?
        get() = getValueAsString("/jhove/repInfo/status")

    @Throws(MetsCreateException::class)
    fun statusOK(): Boolean {
        val expression = "/jhove/repInfo/status"
        return XmlHelper.getNodeValues(document, expression)?.get(0)?.startsWith(okStatus) ?: throw MetsCreateException("Error parsing jhove file, xpath: $expression")

    }

    @get:Throws(MetsCreateException::class)
    val originalName: String?
        get() {
                val expression = "/jhove/repInfo/@uri"
                val file = File(XmlHelper.getNodeValues(document, expression)?.get(0) ?: throw MetsCreateException("Error parsing jhove file, xpath: $expression"))
                return file.name
        }

    @Throws(MetsCreateException::class)
    private fun getValueAsInt(expression: String): Int? {
        return try {
            XmlHelper.getNodeValues(document, expression)?.get(0)?.toInt()
        } catch (ex: NumberFormatException) {
            throw MetsCreateException("Error parsing jhove file, xpath: $expression", ex)
        }
    }

    @Throws(MetsCreateException::class)
    private fun getValueAsBigInt(expression: String): BigInteger? {
        return getValueAsInt(expression)?.toBigInteger()
    }

    @Throws(MetsCreateException::class)
    private fun getValueAsLong(expression: String): Long? {
        return try {
            XmlHelper.getNodeValues(document, expression)?.get(0)?.toLong()
        } catch (ex: NumberFormatException) {
            throw MetsCreateException("Error parsing jhove file, xpath: $expression", ex)
        }
    }

    private fun getValueAsString(expression: String): String? {
        return XmlHelper.getNodeValues(document, expression)?.get(0)
    }

    val documentPageCount: Int
        get() {
            val list: NodeList? =
                XmlHelper.getNodeList(document, "/jhove/repInfo/properties/property/values/property[./name='Pages']/values/property")
            if (list != null && list.length > 1) {
                return list.length
            }
            return 1
        }
}