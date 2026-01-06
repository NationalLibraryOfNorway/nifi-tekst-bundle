package no.nb.nifi.tekst.jhove

import gov.loc.mets.MdSecType
import gov.loc.mets.MdSecType.MdWrap
import gov.loc.mix.v10.ImageAssessmentMetadataType.SpatialMetrics
import gov.loc.mix.v10.Mix
import gov.loc.mix.v10.RationalType
import gov.loc.mix.v10.TypeOfSamplingFrequencyUnitType
import no.nb.nifi.tekst.exceptions.MetsCreateException
import java.io.File

/**
 * Utility class for working with JHOVE output and METS metadata.
 *
 * This class bridges JHOVE output with METS metadata structures,
 * particularly for extracting MIX (Metadata for Images in XML) data.
 */
class JhoveUtil {

    /**
     * Extract MIX metadata from a JHOVE file and populate spatial metrics.
     *
     * @param file The JHOVE XML file to parse
     * @param mix The Mix object to populate with spatial metrics
     * @return true if spatial metrics were successfully extracted
     */
    @Throws(MetsCreateException::class)
    fun getExtraImageDataFromJhove(file: File, mix: Mix): Boolean {
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

    /**
     * Create an MdSecType template with the given payload.
     *
     * @param id The ID for the metadata section
     * @param payload The metadata payload
     * @param mdType The metadata type (e.g., "NISOIMG")
     * @return The constructed MdSecType
     */
    fun getMdSecTemplate(id: String?, payload: Any, mdType: String): MdSecType {
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
