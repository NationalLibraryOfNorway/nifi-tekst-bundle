package no.nb.nifi.tekst.jhove

import no.nb.nifi.tekst.exceptions.MetsCreateException
import no.nb.nifi.tekst.util.XmlHelper
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
    val imageWidth: BigInteger = getValueAsBigInt("//*[local-name()='BasicImageCharacteristics']/*[local-name()='imageWidth']") ?: throw MetsCreateException("Unable to get image width")

    val imageHeigth: BigInteger = getValueAsBigInt("//*[local-name()='BasicImageCharacteristics']/*[local-name()='imageHeight']") ?: throw MetsCreateException("Unable to get image height")

    @Throws(MetsCreateException::class)
    fun samplingFrequencyUnit(): BigInteger? =
        when (fileSuffix) {
            "tif", "tiff", "jpg", "jp2" -> getValueAsBigInt("//*[local-name()='SpatialMetrics']/*[local-name()='samplingFrequencyUnit']")
            else -> null
        }

    @Throws(MetsCreateException::class)
    fun xDenominator(): BigInteger? =
        when (fileSuffix) {
            "tif", "tiff", "jpg", "jp2" -> getValueAsBigInt("//*[local-name()='xSamplingFrequency']/*[local-name()='denominator']")
            else -> null
        }

    @Throws(MetsCreateException::class)
    fun yDenominator(): BigInteger? =
        when (fileSuffix) {
            "tif", "tiff", "jpg", "jp2" -> getValueAsBigInt("//*[local-name()='ySamplingFrequency']/*[local-name()='denominator']")
            else -> null
        }

    @Throws(MetsCreateException::class)
    fun xNumerator(): BigDecimal? =
        when (fileSuffix) {
            "tif", "tiff", "jpg", "jp2" -> {
                val numerator = getValueAsInt("//*[local-name()='xSamplingFrequency']/*[local-name()='numerator']")
                val denominator = getValueAsInt("//*[local-name()='xSamplingFrequency']/*[local-name()='denominator']")
                if (numerator != null && denominator != null && denominator != 0) {
                    numerator.toBigDecimal().divide(denominator.toBigDecimal(), 10, java.math.RoundingMode.HALF_UP)
                } else {
                    numerator?.toBigDecimal()
                }
            }
            else -> null
        }


    @Throws(MetsCreateException::class)
    fun yNumerator(): BigDecimal? =
        when (fileSuffix) {
            "tif", "tiff", "jpg", "jp2" -> {
                val numerator = getValueAsInt("//*[local-name()='ySamplingFrequency']/*[local-name()='numerator']")
                val denominator = getValueAsInt("//*[local-name()='ySamplingFrequency']/*[local-name()='denominator']")
                if (numerator != null && denominator != null && denominator != 0) {
                    numerator.toBigDecimal().divide(denominator.toBigDecimal(), 10, java.math.RoundingMode.HALF_UP)
                } else {
                    numerator?.toBigDecimal()
                }
            }
            else -> null
        }

    val dateLastModified: String = getValueAsString("//*[local-name()='repInfo']/*[local-name()='lastModified']") ?: throw MetsCreateException("Unable to get date last modified")

    @get:Throws(MetsCreateException::class)
    val date: String = getValueAsString("//*[local-name()='jhove']/*[local-name()='date']") ?: throw MetsCreateException("Unable to get date")

    @get:Throws(MetsCreateException::class)
    val size: Long?
        get() = getValueAsLong("//*[local-name()='repInfo']/*[local-name()='size']")

    @get:Throws(MetsCreateException::class)
    val formatName: String?
        get() = getValueAsString("//*[local-name()='repInfo']/*[local-name()='format']")

    @get:Throws(MetsCreateException::class)
    val formatVersion: String?
        get() = getValueAsString("//*[local-name()='repInfo']/*[local-name()='version']")

    @get:Throws(MetsCreateException::class)
    val mD5Checksum: String?
        get() = getValueAsString("//*[local-name()='repInfo']/*[local-name()='checksums']/*[local-name()='checksum'][@type='MD5']")

    @get:Throws(MetsCreateException::class)
    val sHA1Checksum: String?
        get() = getValueAsString("//*[local-name()='repInfo']/*[local-name()='checksums']/*[local-name()='checksum'][@type='SHA-1']")

    val creatingApplication: String?
        get() {
            return when (fileSuffix) {
                "jp2" -> getValueAsString(
                    "//*[local-name()='repInfo']/*[local-name()='properties']/*[local-name()='property']/*[local-name()='values']/*[local-name()='property']/*[local-name()='values']/*[local-name()='property']/*[local-name()='values']/*[local-name()='property'][./*[local-name()='name']='Comments']/*[local-name()='values']/*[local-name()='property'][./*[local-name()='name']='Comment'][1]/*[local-name()='values']/*[local-name()='value']"
                )
                "tif" -> getValueAsString(
                    "//*[local-name()='repInfo']/*[local-name()='properties']/*[local-name()='property']/*[local-name()='values']/*[local-name()='property']/*[local-name()='values']/*[local-name()='property']/*[local-name()='values']/*[local-name()='property'][./*[local-name()='name']='Entries']/*[local-name()='values']/*[local-name()='property'][./*[local-name()='name']='NisoImageMetadata']/*[local-name()='values']/*[local-name()='value']/*[local-name()='mix']/*[local-name()='ImageCaptureMetadata']/*[local-name()='ScannerCapture']/*[local-name()='ScanningSystemSoftware']/*[local-name()='scanningSoftwareName']"
                )
                else -> null
            }
        }

    @get:Throws(MetsCreateException::class)
    val status: String?
        get() = getValueAsString("//*[local-name()='repInfo']/*[local-name()='status']")

    @Throws(MetsCreateException::class)
    fun statusOK(): Boolean {
        val expression = "//*[local-name()='repInfo']/*[local-name()='status']"
        return XmlHelper.getNodeValues(document, expression)?.get(0)?.startsWith(okStatus) ?: throw MetsCreateException("Error parsing jhove file, xpath: $expression")

    }

    @get:Throws(MetsCreateException::class)
    val originalName: String?
        get() {
                val expression = "//*[local-name()='repInfo']/@uri"
                val uriString: String = XmlHelper.getNodeValues(document, expression)?.get(0) ?: throw MetsCreateException("Error parsing jhove file, xpath: $expression")
                val file = File(uriString)
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
}
