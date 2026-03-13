package no.nb.nifi.tekst.processors

import no.nb.nifi.tekst.util.XmlHelper
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.w3c.dom.Document
import org.w3c.dom.NodeList
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Integration test that chains the Jhove processor with the CreateMetsBrowsing processor (METS_2).
 */
class JhoveToMets2BrowsingIntegrationTest {

    private lateinit var objectFolder: Path
    private lateinit var tempMetsOutputFile: File
    private val projectFolder = Paths.get("").toAbsolutePath().toString()

    private val noJhoveRoot = Paths.get(
        projectFolder,
        "src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_no_jhove"
    )
    private val descriptiveXml = Paths.get(
        projectFolder,
        "src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72-METS2_BROWSING.xml"
    )

    private val expectedMetsFile = File(
        "$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72-METS2_BROWSING.xml"
    )

    private val objectId = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_no_jhove"

    @BeforeEach
    fun setup() {
        objectFolder = TestObjectFolderHelper.createTempObjectFolder(noJhoveRoot, descriptiveXml)
        tempMetsOutputFile = File.createTempFile("mets2_integration_test", ".xml")
    }

    @AfterEach
    fun cleanup() {
        TestObjectFolderHelper.deleteTempObjectFolder(objectFolder)
        if (tempMetsOutputFile.exists()) {
            tempMetsOutputFile.delete()
        }
    }

    @Test
    fun testCompleteJhoveToMets2BrowsingWorkflow() {
        val jhoveRunner = TestRunners.newTestRunner(Jhove::class.java)
        jhoveRunner.setProperty(Jhove.OBJECT_FOLDER, objectFolder.toString())
        jhoveRunner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        jhoveRunner.enqueue("test")
        jhoveRunner.run()

        jhoveRunner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, 1)

        val accessJhoveOutputDir = objectFolder.resolve("representations/access/metadata/technical/jhove").toFile()
        val jhoveOutputFiles = accessJhoveOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }?.sortedBy { it.name }

        assertNotNull(jhoveOutputFiles, "JHOVE output files should exist")
        assertEquals(4, jhoveOutputFiles?.size, "Should have JHOVE output for each input image")

        val metsRunner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)
        metsRunner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder.toString())
        metsRunner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, "representations/access/metadata/other/ocr")
        metsRunner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, "representations/access/data")
        metsRunner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, "representations/access/metadata/technical/jhove")
        metsRunner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, tempMetsOutputFile.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        metsRunner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        metsRunner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        metsRunner.enqueue("test")
        metsRunner.run()

        metsRunner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(tempMetsOutputFile.exists(), "METS output file should exist")
        assertTrue(tempMetsOutputFile.length() > 0, "METS output file should not be empty")

        val generatedDoc = parseXmlFile(tempMetsOutputFile)
        val expectedDoc = parseXmlFile(expectedMetsFile)

        assertEquals("mets", generatedDoc.documentElement.localName, "Root element should be 'mets'")
        assertEquals(objectId, generatedDoc.documentElement.getAttribute("OBJID"), "OBJID should match")

        val generatedPages = xpathNodeList(generatedDoc, "//*[local-name()='div'][@TYPE='PAGE']")
        val expectedPages = xpathNodeList(expectedDoc, "//*[local-name()='div'][@TYPE='PAGE']")
        assertEquals(expectedPages.length, generatedPages.length, "Number of pages should match expected output")
        assertEquals(4, generatedPages.length, "Should have 4 pages")
    }

    @Test
    fun testIntegrationWithMissingJhoveFile() {
        val jhoveRunner = TestRunners.newTestRunner(Jhove::class.java)
        jhoveRunner.setProperty(Jhove.OBJECT_FOLDER, objectFolder.toString())
        jhoveRunner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        jhoveRunner.enqueue("test")
        jhoveRunner.run()

        val accessJhoveOutputDir = objectFolder.resolve("representations/access/metadata/technical/jhove").toFile()
        val jhoveOutputFiles = accessJhoveOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }?.sortedBy { it.name }

        assertNotNull(jhoveOutputFiles, "JHOVE output files should exist")
        assertEquals(4, jhoveOutputFiles?.size, "Should have 4 JHOVE files before deletion")

        jhoveOutputFiles!![0].delete()

        val metsRunner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)
        metsRunner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder.toString())
        metsRunner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, "representations/access/metadata/other/ocr")
        metsRunner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, "representations/access/data")
        metsRunner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, "representations/access/metadata/technical/jhove")
        metsRunner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, tempMetsOutputFile.absolutePath)

        metsRunner.enqueue("test")
        metsRunner.run()

        metsRunner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_FAILURE)
        assertFalse(tempMetsOutputFile.exists() && tempMetsOutputFile.length() > 1000,
            "METS output file should not be successfully created when JHOVE files are missing")
    }

    @Test
    fun testIntegrationPerformanceMetrics() {
        val startTime = System.currentTimeMillis()

        val jhoveRunner = TestRunners.newTestRunner(Jhove::class.java)
        jhoveRunner.setProperty(Jhove.OBJECT_FOLDER, objectFolder.toString())
        jhoveRunner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val jhoveStartTime = System.currentTimeMillis()
        jhoveRunner.enqueue("test")
        jhoveRunner.run()
        val jhoveEndTime = System.currentTimeMillis()

        val metsRunner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)
        metsRunner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder.toString())
        metsRunner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, "representations/access/metadata/other/ocr")
        metsRunner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, "representations/access/data")
        metsRunner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, "representations/access/metadata/technical/jhove")
        metsRunner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, tempMetsOutputFile.absolutePath)

        val metsStartTime = System.currentTimeMillis()
        metsRunner.enqueue("test")
        metsRunner.run()
        val metsEndTime = System.currentTimeMillis()

        val totalTime = System.currentTimeMillis() - startTime

        println("=== Integration Test Performance Metrics ===")
        println("Jhove processing time: ${jhoveEndTime - jhoveStartTime}ms")
        println("METS generation time: ${metsEndTime - metsStartTime}ms")
        println("Total workflow time: ${totalTime}ms")

        assertTrue(totalTime < 120000, "Total workflow should complete in under 2 minutes")
    }

    private fun parseXmlFile(file: File): Document = XmlHelper.parseXmlFile(file)

    private fun xpathNodeList(doc: Document, expression: String): NodeList = XmlHelper.xpathNodeList(doc, expression)
}
