package no.nb.nifi.tekst.mets

import gov.loc.mets.MdSecType
import gov.loc.mets.MdSecType.MdWrap
import gov.loc.mix.v10.ImageAssessmentMetadataType.SpatialMetrics
import gov.loc.mix.v10.Mix
import gov.loc.mix.v10.RationalType
import gov.loc.mix.v10.TypeOfSamplingFrequencyUnitType
import no.nb.nifi.tekst.exception.MetsCreateException
import java.io.File
import java.math.BigDecimal
import java.math.BigInteger

class JhoveUtil {
    @Throws(MetsCreateException::class)
    fun getMixDataFromJhove(file: File, id: String?, stripMix: Boolean, trimBox: TrimBox?): MdSecType {
        val mixObject: Any = getObjectFromFile(
            file,
            MIX_XPATH_JHOVE
        )
        if (mixObject is Mix) {
            val mix = mixObject
            if (mix.imageAssessmentMetadata.spatialMetrics == null) { // Det ligger ikke spatial metrics i MIX-tag.
                if (!getExtraImageDataFromJhove(
                        file,
                        mix
                    )
                ) { // Vi klarte ikke å hente ut spatial metrics fra JHOVE JP2, forsøk JHOVE TIFF.
                    val searchPattern = file.name.replaceFirst(".jp2.xml".toRegex(), "") + "\\.(tif|tiff)\\.xml"
                    val filter: DirFilter = DirFilter()
                    filter.addFilter(FilterDefinition(FilterDefinition.fileType.FILE, FilterDefinition.filterType.REGEXP, searchPattern))
                    val filesFound: Array<File> = file.parentFile.listFiles(filter)
                    if (filesFound != null) {
                        getExtraImageDataFromJhove(filesFound[0], mix)
                    }
                }
            }

            if (stripMix) {
                //Strips mixData in AmdSecResolution
                mix.basicDigitalObjectInformation = null
                mix.imageCaptureMetadata = null
                mix.imageAssessmentMetadata.imageColorEncoding = null
                mix.imageAssessmentMetadata.targetData = null
                mix.basicImageInformation.specialFormatCharacteristics = null
                mix.basicImageInformation.basicImageCharacteristics.photometricInterpretation = null
            }
            return getMdSecTemplate(id, mix, "NISOIMG")
        } else {
            throw MetsCreateException("Finner ikke MIX-data i JHOVE-fil for " + file.absolutePath)
        }
    }

    @Throws(MetsCreateException::class)
    private fun getExtraImageDataFromJhove(file: File, mix: Mix): Boolean {
        val jhoveParser = JhoveParser(file)
        var retVal = true // TRUE hvis vi finner spatialMetrics i JHOVE-fila.
        try {
            if (jhoveParser.fileSuffix.matches("""jp2|tif|tiff""".toRegex())) {
                val spatialMetrics = SpatialMetrics()

                val tosfut = TypeOfSamplingFrequencyUnitType()
                tosfut.value = jhoveParser.samplingFrequencyUnit()
                spatialMetrics.samplingFrequencyUnit = tosfut

                var rt = RationalType()
                if (jhoveParser.xDenominator() != null) {
                    rt.denominator = jhoveParser.xDenominator()
                }
                rt.numerator = jhoveParser.xNumerator()
                spatialMetrics.xSamplingFrequency = rt

                rt = RationalType()
                if (jhoveParser.yDenominator() != null) {
                    rt.denominator = jhoveParser.yDenominator()
                }
                rt.numerator = jhoveParser.yNumerator()
                spatialMetrics.ySamplingFrequency = rt
                mix.imageAssessmentMetadata.spatialMetrics = spatialMetrics
            }
        } catch (npe: NullPointerException) {
            retVal = false
        }
        return retVal
    }

    private fun getMdSecTemplate(id: String, payload: Any, mdType: String): MdSecType {
        val mdSec = MdSecType()
        mdSec.id = id

        val mdWrap = MdWrap()
        mdWrap.mdtype = mdType
        mdSec.mdWrap = mdWrap

        val xmlData = MdWrap.XmlData()
        mdWrap.xmlData = xmlData
        xmlData.any.add(payload)
        return mdSec
    }
}