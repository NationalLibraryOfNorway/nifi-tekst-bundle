package no.nb.nifi.tekst.mets

import gov.loc.mets.AmdSecType
import gov.loc.mets.MdSecType
import gov.loc.mets.MdSecType.MdWrap
import gov.loc.mets.MetsType
import gov.loc.mets.MetsType.FileSec
import gov.loc.mets.MetsType.MetsHdr
import gov.loc.mets.StructMapType
import gov.loc.mix.v10.*
import gov.loc.mix.v10.BasicImageInformationType.SpecialFormatCharacteristics.JPEG2000.EncodingOptions
import gov.loc.mix.v10.ImageAssessmentMetadataType.SpatialMetrics
import jakarta.xml.bind.JAXBException
import no.nb.nifi.tekst.exception.MetsCreateException
import org.w3c.dom.Node
import java.io.File
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*
import javax.xml.datatype.DatatypeConfigurationException
import javax.xml.datatype.DatatypeFactory

const val MIX_XPATH_JHOVE: String = "//jhove:property[jhove:name='NisoImageMetadata']/jhove:values/jhove:value/mix:mix"

class MetsBrowsingGenerator {

    fun generateMetsBrowsing(objectFolder: String) {
        val mets = MetsType()
        mets.objid = objectFolder.split("/").last()
        mets.metsHdr = createMetsHeader()
        // TODO comparator where?
        mets.amdSec.add(createContainerAmdSecEncoding())
        createContainerAmdSecResolution().forEach {
            mets.amdSec.add(it)
        }
        mets.fileSec = createSectionFileSec()
        mets.structMap.add(createSectionStructMapPhysical())
    }


    private fun createMetsHeader(): MetsHdr? {
        try {
            val header = MetsHdr()
            val xcal = DatatypeFactory.newInstance().newXMLGregorianCalendar(GregorianCalendar())
            header.createdate = xcal
            header.lastmoddate = xcal

            val properties = Properties()
            properties.load(this.javaClass.getResourceAsStream("project.properties"))
            val metsCreatingAgent = properties.getProperty("groupId") + "." + properties.getProperty("artifactId") + " v" + properties.getProperty("version")

            val agent = MetsHdr.Agent()
            agent.name = metsCreatingAgent
            agent.role = "CREATOR"
            agent.type = "OTHER"
            agent.othertype = "Software"
            header.agent.add(agent)

            return header
        } catch (ex: DatatypeConfigurationException) {
            return null
        }
    }

    private fun createContainerAmdSecEncoding(): AmdSecType {
        val amdSec = AmdSecType()
        val mdSec = MdSecType()
        val mdWrap = MdWrap()
        val xmlData = MdWrap.XmlData()
        val mix = Mix()
        val biit = BasicImageInformationType()
        val sfc: BasicImageInformationType.SpecialFormatCharacteristics = BasicImageInformationType.SpecialFormatCharacteristics()
        val jp2k: BasicImageInformationType.SpecialFormatCharacteristics.JPEG2000 = BasicImageInformationType.SpecialFormatCharacteristics.JPEG2000()
        val eo: EncodingOptions = buildEncodingOptions()

        jp2k.setEncodingOptions(eo)
        sfc.setJPEG2000(jp2k)
        biit.setSpecialFormatCharacteristics(sfc)
        mix.setBasicImageInformation(biit)
        xmlData.any.add(mix)

        mdWrap.mdtype = "NISOIMG"
        mdWrap.xmlData = xmlData

        mdSec.id = "TM_J2K"
        mdSec.mdWrap = mdWrap

        amdSec.id = "JPEG2000_ENCODING"
        amdSec.techMD.add(mdSec)

        return amdSec
    }

    private fun buildEncodingOptions(): EncodingOptions {
        val eo = EncodingOptions()

        val stXyTiles = StringType()
        stXyTiles.value = "1024x1024"
        eo.tiles = stXyTiles

        val piQualityLayers = PositiveIntegerType()
        piQualityLayers.value = BigInteger.valueOf(13)
        eo.qualityLayers = piQualityLayers

        val piResolutionLevels = PositiveIntegerType()
        piResolutionLevels.value = BigInteger.valueOf(6)
        eo.resolutionLevels = piResolutionLevels

        return eo
    }

    private fun createContainerAmdSecResolution(): List<AmdSecType>{

        /***
         * Amd_SecResolution legges til METS-fila kun for å kunne vise frem objektet riktig, dvs. det vi legger inn
         * er oppløsninga til JPEG 2000 visningsfil. Egentlig burde ikke denne seksjonen vært med i METS-fila.
         */
        val jhoveFile: File = getFileByPattern(inputDir, "JHOVE_$baseFile\\.$suffix\\.xml")

        val amdSec = AmdSecType()

        //TODO: Remove hardcode RESOLUTION. Fetch from Contentpart
        amdSec.id = "RESOLUTION_P" + getIdSequence(sectionNumber)
        val mdSec: MdSecType = getMixDataFromJhove(jhoveFile, "TM_" + String.format("%04d", sectionNumber + 1), true, trimBox)
        amdSec.techMD.add(mdSec)

        return amdSec

    }


    @Throws(MetsCreateException::class)
    private fun getObjectFromFile(file: File, xpath: String): Any {
        try {
            val xmlNode: Node = loadNodeFromFile(file, xpath).item(0)

            val metaUtil: MetadataUtils = MetadataUtils.getInstance()
            metaUtil.setValidating(false)
            return metaUtil.createUnmarshaller().unmarshal(xmlNode)
        } catch (ex: JAXBException) {
            throw MetsCreateException("Feil under unmarshalling fra xml til objekt: " + file.absolutePath, ex)
        }
    }

    private fun createSectionFileSec(): FileSec {

    }

    private fun createSectionStructMapPhysical(): StructMapType {

    }



}