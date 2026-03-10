package no.nb.nifi.tekst.processors

import no.nb.nifi.tekst.util.XmlHelper
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.w3c.dom.Document
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import javax.xml.parsers.DocumentBuilderFactory

class JhoveTest {

    private lateinit var objectFolder: Path
    private val projectFolder = Paths.get("").toAbsolutePath().toString()
    private val fixtureRoot = Paths.get(projectFolder, "src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72")
    private val descriptiveXml = Paths.get(
        projectFolder,
        "src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72-METS_BROWSING.xml"
    )

    @BeforeEach
    fun setup() {
        objectFolder = TestObjectFolderHelper.createTempObjectFolder(fixtureRoot, descriptiveXml)
    }

    @AfterEach
    fun cleanup() {
        TestObjectFolderHelper.deleteTempObjectFolder(objectFolder)
    }

    @Test
    fun testValidProcessingCreatesJhoveOutputs() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.OBJECT_FOLDER, objectFolder.toString())
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        runner.enqueue("test")
        runner.run()

        runner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, 1)

        val primaryOutputDir = objectFolder.resolve("representations/primary/metadata/technical/jhove").toFile()
        val accessOutputDir = objectFolder.resolve("representations/access/metadata/technical/jhove").toFile()
        val ocrOutputDir = objectFolder.resolve("representations/access/metadata/other/jhove-ocr").toFile()
        val descriptiveOutputDir = objectFolder.resolve("metadata/other/jhove").toFile()

        assertTrue(primaryOutputDir.exists(), "Primary JHOVE output dir should exist")
        assertTrue(accessOutputDir.exists(), "Access JHOVE output dir should exist")
        assertTrue(ocrOutputDir.exists(), "OCR JHOVE output dir should exist")
        assertTrue(descriptiveOutputDir.exists(), "Descriptive JHOVE output dir should exist")

        assertEquals(4, primaryOutputDir.listFiles { file -> file.name.endsWith(".xml") }?.size)
        assertEquals(4, accessOutputDir.listFiles { file -> file.name.endsWith(".xml") }?.size)
        assertEquals(4, ocrOutputDir.listFiles { file -> file.name.endsWith(".xml") }?.size)
        assertEquals(1, descriptiveOutputDir.listFiles { file -> file.name.endsWith(".xml") }?.size)
    }

    @Test
    fun testJhoveXmlOutputStructure() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.OBJECT_FOLDER, objectFolder.toString())
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        runner.enqueue("test")
        runner.run()

        val primaryOutputDir = objectFolder.resolve("representations/primary/metadata/technical/jhove").toFile()
        val jhoveOutputFile = primaryOutputDir.listFiles { file -> file.name.startsWith("JHOVE_") }?.firstOrNull()
        assertNotNull(jhoveOutputFile, "JHOVE output file should exist")

        val doc = parseXmlFile(jhoveOutputFile!!)

        assertEquals("jhove", doc.documentElement.localName, "Root element should be 'jhove'")

        val status = xpath(doc, "//jhove:repInfo/jhove:status")
        assertNotNull(status, "Status element should exist")
        assertTrue(status!!.startsWith("Well-Formed"), "Status should indicate well-formed file")

        val format = xpath(doc, "//jhove:repInfo/jhove:format")
        assertEquals("JPEG 2000", format, "Format should be JPEG 2000")
    }

    @Test
    fun testJhoveOutputFilenamePattern() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.OBJECT_FOLDER, objectFolder.toString())
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        runner.enqueue("test")
        runner.run()

        val primaryOutputDir = objectFolder.resolve("representations/primary/metadata/technical/jhove").toFile()
        val outputFiles = primaryOutputDir.listFiles { file -> file.name.startsWith("JHOVE_") } ?: emptyArray()
        assertTrue(outputFiles.isNotEmpty(), "Should have JHOVE output files")
        assertTrue(outputFiles.all { it.name.endsWith(".xml") }, "All JHOVE output files should end with .xml")
    }

    @Test
    fun testMissingObjectFolderFails() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.OBJECT_FOLDER, "/nonexistent/path")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        runner.enqueue("test")
        runner.run()

        runner.assertTransferCount(Jhove.FAIL_RELATIONSHIP, 1)
        val failedFlowFile = runner.getFlowFilesForRelationship(Jhove.FAIL_RELATIONSHIP).first()
        assertNotNull(failedFlowFile.getAttribute("error.message"))
    }

    @Test
    fun testContinueOnErrorModeWithInvalidXml() {
        val invalidXml = objectFolder.resolve("metadata/descriptive/invalid.xml")
        Files.writeString(invalidXml, "<invalid>")

        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.OBJECT_FOLDER, objectFolder.toString())
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "continue")

        runner.enqueue("test")
        runner.run()

        runner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, 1)
    }

    @Test
    fun testEmptyObjectFolderRoutesToEmpty() {
        val emptyObjectFolder = Files.createTempDirectory("jhove_empty_")
        try {
            val runner = TestRunners.newTestRunner(Jhove::class.java)

            runner.setProperty(Jhove.OBJECT_FOLDER, emptyObjectFolder.toString())
            runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

            runner.enqueue("test")
            runner.run()

            runner.assertTransferCount(Jhove.EMPTY_RESULT_RELATIONSHIP, 1)
            val flowFile = runner.getFlowFilesForRelationship(Jhove.EMPTY_RESULT_RELATIONSHIP).first()
            assertEquals("0", flowFile.getAttribute("jhove.files_processed"))
        } finally {
            emptyObjectFolder.toFile().deleteRecursively()
        }
    }

    @Test
    fun testReadOnlyTargetFolderRoutesToFailure() {
        // Create a file where the target directory would need to be created,
        // making it impossible to create subdirectories
        val blockingFile = objectFolder.resolve("representations/primary/metadata/technical/jhove")
        Files.createDirectories(blockingFile.parent)
        Files.writeString(blockingFile, "blocker") // regular file where a directory is expected

        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.OBJECT_FOLDER, objectFolder.toString())
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        runner.enqueue("test")
        runner.run()

        runner.assertTransferCount(Jhove.FAIL_RELATIONSHIP, 1)
        val failedFlowFile = runner.getFlowFilesForRelationship(Jhove.FAIL_RELATIONSHIP).first()
        assertNotNull(failedFlowFile.getAttribute("error.message"))
    }

    @Test
    fun testFlowFileAttributesContainValidationSummary() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.OBJECT_FOLDER, objectFolder.toString())
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        runner.enqueue("test")
        runner.run()

        runner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, 1)
        val flowFile = runner.getFlowFilesForRelationship(Jhove.SUCCESS_RELATIONSHIP).first()

        assertNotNull(flowFile.getAttribute("jhove.files_processed"))
        assertTrue(flowFile.getAttribute("jhove.files_processed")!!.toInt() > 0,
            "Should have processed at least one file")
        assertEquals("true", flowFile.getAttribute("jhove.all_valid"))
        assertEquals("true", flowFile.getAttribute("jhove.all_wellformed"))
    }

    private fun parseXmlFile(file: File): Document {
        val factory = DocumentBuilderFactory.newInstance()
        factory.isNamespaceAware = true
        val builder = factory.newDocumentBuilder()
        return builder.parse(file)
    }

    private fun xpath(doc: Document, expr: String): String? {
        return XmlHelper.xpath(doc, expr)
    }
}
