package no.nb.nifi.tekst.metssections

import com.itextpdf.text.Rectangle
import com.itextpdf.text.pdf.PdfReader
import gov.loc.mets.AmdSecType
import gov.loc.mets.MdSecType
import no.nb.commons.metadata.util.NamespacePrefixConstants
import no.nb.nifi.tekst.exception.MetsCreateException
import no.nb.productioncontroller.task.metscreate.MetsCreateException
import no.nb.productioncontroller.task.metscreate.MetsSectionParameter
import no.nb.productioncontroller.task.metscreate.util.ContentList
import no.nb.productioncontroller.task.metscreate.util.ContentPart
import no.nb.productioncontroller.task.metscreate.util.MdSecHelper
import no.nb.productioncontroller.task.metscreate.util.TrimBox
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException

/**
 * @author Thomas Edvardsen &lt;thomas.edvardsen@nb.no&gt;
 */
class ContainerAmdSecResolution : AbstractMetsSection() {
    @MetsSectionParameter(optional = false)
    var browsingSeqRef: Int = 0

    @Throws(MetsCreateException::class)
    override fun buildSection(workingdir: File): List<AmdSecType> {
        addNamespace(NamespacePrefixConstants.MIX)
        val sections: MutableList<AmdSecType> = ArrayList()
        val contentList: ContentList = contentLists[contentListSequence]
        val browsingContentPart: ContentPart = contentList.getContentPartBySeq(browsingSeqRef)
            ?: throw MetsCreateException("Invalid config for ContainerAmdSecResolution. Could not find ContentPart with seq:'$browsingSeqRef'")

        if (pageIndex === -1) {
            for (i in 0..<contentList.getFileList().length) {
                sections.add(getAmdSecType(workingdir, browsingContentPart, i, i))
            }
        } else {
            sections.add(getAmdSecType(workingdir, browsingContentPart, pageIndex, 0))
            if (contentList.isDualpage()) {
                sections.add(getAmdSecType(workingdir, browsingContentPart, pageIndex + 1, 1))
            }
        }

        return sections
    }


    @Throws(MetsCreateException::class)
    private fun getAmdSecType(workingdir: File, contentPart: ContentPart, pIndex: Int, sectionNumber: Int): AmdSecType {
        val inputDir: File = contentPart.getInputDir()
        val contentList: ContentList = contentLists[contentListSequence]
        var trimBox: TrimBox? = null

        val baseFile: String = contentList.getFileList().get(pIndex).getFileId()
        val suffix: String = contentPart.getSuffix()


        // Finn ut om vi har PDF-katalog, vi må i så fall sjekke om det er trimbox på pdf-varianten av visningsfilene, denne må med i MIX-data.
        var pdfDir = File(workingdir, "pages")
        if (!pdfDir.exists()) {
            pdfDir = File(workingdir, "pdf")
        }
        if (pdfDir.exists()) {
            var pdfPath: String? = null
            try {
                pdfPath = pdfDir.canonicalPath + File.separator + baseFile + ".pdf"
                val reader: PdfReader = PdfReader(pdfPath)


                val rect: Rectangle = reader.getBoxSize(1, "trim")
                if (rect != null && !rect.equals(reader.getPageSize(1))) {
                    trimBox = TrimBox(rect.getLeft(), reader.getPageSize(1).getHeight() - rect.getTop(), rect.getWidth(), rect.getHeight())
                }
            } catch (ioe: IOException) {
                // Do nothing, just means that there is no trimbox
            }
        }

        /***
         * Amd_SecResolution legges til METS-fila kun for å kunne vise frem objektet riktig, dvs. det vi legger inn
         * er oppløsninga til JPEG 2000 visningsfil. Egentlig burde ikke denne seksjonen vært med i METS-fila.
         */
        val jhoveFile = super.getFileByPattern(inputDir, "JHOVE_$baseFile\\.$suffix\\.xml")
        LOGGER.debug("Parsing Jhove file: {}", jhoveFile!!.name)

        val amdSec = AmdSecType()
        //TODO: Remove hardcode RESOLUTION. Fetch from Contentpart
        amdSec.id = "RESOLUTION_P" + getIdSequence(sectionNumber)
        val mdSec: MdSecType = MdSecHelper.getMixDataFromJhove(jhoveFile, "TM_" + String.format("%04d", sectionNumber + 1), true, trimBox)
        amdSec.techMD.add(mdSec)

        return amdSec
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(ContainerAmdSecResolution::class.java)
    }
}
