package no.nb.nifi.tekst.processors

import no.nb.nifi.tekst.util.XmlHelper
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

class CreateMetsBrowsingTest {

    private lateinit var outputFile: File
    private val projectFolder = Paths.get("").toAbsolutePath().toString()
    private val expectedFile = File("$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72-METS_BROWSING.xml")
    private val objectFolder = "$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72"
    private val altoFolderRelative = "access/metadata/other/ocr"
    private val imageFolderRelative = "access/data"
    private val jhoveFolderRelative = "access/metadata/technical/jhove"
    private val objectId = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72"

    @BeforeEach
    fun setup() {
        outputFile = File.createTempFile("test_mets_browsing", ".xml")
    }

    @AfterEach
    fun cleanup() {
        if (outputFile.exists()) {
            outputFile.delete()
        }
    }

    @Test
    fun testSuccessfulMetsBrowsingGeneration() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(outputFile.exists(), "Output file should exist")
        assertTrue(outputFile.length() > 0, "Output file should not be empty")
    }

    @Test
    fun testGeneratedMetsStructure() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        runner.enqueue("test")
        runner.run()

        // Parse the generated XML
        val doc = parseXmlFile(outputFile)

        // Verify root element
        assertEquals("mets", doc.documentElement.localName)
        assertEquals(objectId, doc.documentElement.getAttribute("OBJID"))

        // Verify metsHdr exists
        val metsHdr = xpath(doc, "//*[local-name()='metsHdr']")
        assertNotNull(metsHdr, "metsHdr should exist")

        // Verify agent name
        val agentName = xpath(doc, "//*[local-name()='agent']/*[local-name()='name']/text()")
        assertEquals("MetsBrowsingGenerator v1", agentName)

        // Verify JPEG2000 encoding section exists
        val jpeg2000Encoding = xpath(doc, "//*[@ID='JPEG2000_ENCODING']")
        assertNotNull(jpeg2000Encoding, "JPEG2000_ENCODING should exist")

        // Verify resolution sections exist for all 4 pages
        val resolution1 = xpath(doc, "//*[@ID='RESOLUTION_P0001']")
        assertNotNull(resolution1, "RESOLUTION_P0001 should exist")

        val resolution4 = xpath(doc, "//*[@ID='RESOLUTION_P0004']")
        assertNotNull(resolution4, "RESOLUTION_P0004 should exist")

        // Verify fileSec has correct structure
        val fileGrp = xpath(doc, "//*[local-name()='fileGrp'][@ID='BROWSINGGRP']")
        assertNotNull(fileGrp, "BROWSINGGRP should exist")

        // Verify we have 4 DO entries (top-level only)
        val doEntries = xpathNodeList(doc, "//*[local-name()='fileGrp']/*[local-name()='file'][starts-with(@ID, 'DO_')]")
        assertEquals(4, doEntries.length, "Should have 4 DO entries")

        // Verify structMap has correct structure
        val structMap = xpath(doc, "//*[local-name()='structMap'][@TYPE='PHYSICAL']")
        assertNotNull(structMap, "PHYSICAL structMap should exist")

        // Verify we have 4 page divs
        val pageDivs = xpathNodeList(doc, "//*[local-name()='div'][@TYPE='PAGE']")
        assertEquals(4, pageDivs.length, "Should have 4 PAGE divs")
    }

    @Test
    fun testGeneratedMetsChecksums() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        runner.enqueue("test")
        runner.run()

        val doc = parseXmlFile(outputFile)

        // Verify checksums exist (actual values will be checked in output)
        val alto1Checksum = xpath(doc, "//*[@ID='ALTO_0001']/@CHECKSUM")
        assertNotNull(alto1Checksum, "ALTO 0001 checksum should exist")
        assertTrue(alto1Checksum!!.length == 32, "ALTO 0001 checksum should be MD5 format")

        val alto2Checksum = xpath(doc, "//*[@ID='ALTO_0002']/@CHECKSUM")
        assertNotNull(alto2Checksum, "ALTO 0002 checksum should exist")
    }

    @Test
    fun testGeneratedMetsFileSize() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        runner.enqueue("test")
        runner.run()

        val doc = parseXmlFile(outputFile)

        // Verify file sizes exist and are > 0
        val alto1Size = xpath(doc, "//*[@ID='ALTO_0001']/@SIZE")
        assertNotNull(alto1Size, "ALTO 0001 size should exist")
        assertTrue(alto1Size!!.toLong() > 0, "ALTO 0001 size should be greater than 0")
    }

    @Test
    fun testMissingAltoFolder() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, "nonexistent/folder")
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_FAILURE)
    }

    @Test
    fun testMissingImageFolder() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, "nonexistent/folder")
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_FAILURE)
    }

    @Test
    fun testEmptyAltoFolder() {
        // Create a temp object folder with an empty alto subdirectory
        val tempObjectFolder = Files.createTempDirectory("test_object").toFile()
        val emptyAltoFolder = File(tempObjectFolder, "empty_alto")
        emptyAltoFolder.mkdirs()
        // Also need image and jhove folders for the test to reach the alto check
        val imageDir = File(tempObjectFolder, imageFolderRelative)
        val jhoveDir = File(tempObjectFolder, jhoveFolderRelative)
        imageDir.mkdirs()
        jhoveDir.mkdirs()

        try {
            val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

            runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, tempObjectFolder.absolutePath)
            runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, "empty_alto")
            runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
            runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
            runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
            runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
            runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
            runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

            runner.enqueue("test")
            runner.run()

            runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_FAILURE)
        } finally {
            tempObjectFolder.deleteRecursively()
        }
    }

    @Test
    fun testGeneratedMetsUrns() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        runner.enqueue("test")
        runner.run()

        val doc = parseXmlFile(outputFile)

        // Verify URNs are correctly formatted
        val alto1Urn = xpath(doc, "//*[@ID='ALTO_0001']/*[@LOCTYPE='URN']/@*[local-name()='href']")
        assertEquals("URN:NBN:no-nb_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001", alto1Urn, "ALTO 0001 URN should be correct")
    }


    @Test
    fun testMatchesExpectedOutput() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(outputFile.exists(), "Output file should exist")

        // Parse both files
        val generated = parseXmlFile(outputFile)
        val expected = parseXmlFile(expectedFile)

        // Compare key structure elements
        val generatedObjId = generated.documentElement.getAttribute("OBJID")
        val expectedObjId = expected.documentElement.getAttribute("OBJID")
        assertEquals(expectedObjId, generatedObjId, "OBJID should match")

        // Compare number of pages
        val generatedPages = xpathNodeList(generated, "//*[local-name()='div'][@TYPE='PAGE']")
        val expectedPages = xpathNodeList(expected, "//*[local-name()='div'][@TYPE='PAGE']")
        assertEquals(expectedPages.length, generatedPages.length, "Number of pages should match")

        // Compare image dimensions for page 1
        val generatedWidth1 = xpath(generated, "//*[@ID='TM_0001']//*[local-name()='imageWidth']/text()")
        val expectedWidth1 = xpath(expected, "//*[@ID='TM_0001']//*[local-name()='imageWidth']/text()")
        assertEquals(expectedWidth1, generatedWidth1, "Page 1 width should match")

        val generatedHeight1 = xpath(generated, "//*[@ID='TM_0001']//*[local-name()='imageHeight']/text()")
        val expectedHeight1 = xpath(expected, "//*[@ID='TM_0001']//*[local-name()='imageHeight']/text()")
        assertEquals(expectedHeight1, generatedHeight1, "Page 1 height should match")

        // Compare image dimensions for page 2
        val generatedWidth2 = xpath(generated, "//*[@ID='TM_0002']//*[local-name()='imageWidth']/text()")
        val expectedWidth2 = xpath(expected, "//*[@ID='TM_0002']//*[local-name()='imageWidth']/text()")
        assertEquals(expectedWidth2, generatedWidth2, "Page 2 width should match")

        // Compare checksums (they should be identical if files are the same)
        val generatedAlto1Checksum = xpath(generated, "//*[@ID='ALTO_0001']/@CHECKSUM")
        val expectedAlto1Checksum = xpath(expected, "//*[@ID='ALTO_0001']/@CHECKSUM")
        assertEquals(expectedAlto1Checksum, generatedAlto1Checksum, "ALTO 1 checksum should match")

        val generatedImg1Checksum = xpath(generated, "//*[@ID='IMG_0001']/@CHECKSUM")
        val expectedImg1Checksum = xpath(expected, "//*[@ID='IMG_0001']/@CHECKSUM")
        assertEquals(expectedImg1Checksum, generatedImg1Checksum, "IMG 1 checksum should match")

        // Compare file URLs
        val generatedAlto1Url = xpath(generated, "//*[@ID='ALTO_0001']/*[@LOCTYPE='URL']/@*[local-name()='href']")
        val expectedAlto1Url = xpath(expected, "//*[@ID='ALTO_0001']/*[@LOCTYPE='URL']/@*[local-name()='href']")
        assertEquals(expectedAlto1Url, generatedAlto1Url, "ALTO 1 URL should match")
    }

    @Test
    fun testTekstSIPDebugOutput() {
        val debugOutput = File("/tmp/debug_mets_output.xml")

        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, debugOutput.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(debugOutput.exists(), "Output file should exist")

        println("Generated METS file saved to: ${debugOutput.absolutePath}")
        println("File size: ${debugOutput.length()} bytes")
    }


    @Test
    fun testTekstPrettyPrintComparison() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(outputFile.exists(), "Output file should exist")

        // Read both files as text
        var generatedContent = outputFile.readText()
        var expectedContent = expectedFile.readText()

        // Normalize timestamps in both generated and expected content
        generatedContent = normalizeTimestampsInText(generatedContent)
        expectedContent = normalizeTimestampsInText(expectedContent)

        // Compare line by line for exact match including whitespace
        val generatedLines = generatedContent.lines()
        val expectedLines = expectedContent.lines()

        val differences = mutableListOf<String>()
        val maxLines = maxOf(expectedLines.size, generatedLines.size)

        for (i in 0 until maxLines) {
            val expectedLine = expectedLines.getOrNull(i) ?: ""
            val generatedLine = generatedLines.getOrNull(i) ?: ""

            if (expectedLine != generatedLine) {
                differences.add("Line ${i + 1}:")
                differences.add("  Expected:  '$expectedLine'")
                differences.add("  Generated: '$generatedLine'")
                differences.add("")
            }
        }

        if (differences.isNotEmpty()) {
            println("\n" + "=".repeat(80))
            println("TEXT DIFFERENCES FOUND (${differences.size / 4} lines differ)")
            println("=".repeat(80))
            differences.take(100).forEach { println(it) }
            if (differences.size > 100) {
                println("... and ${differences.size - 100} more difference lines")
            }
            println("=".repeat(80) + "\n")
        }

        assertEquals(expectedContent, generatedContent,
            "Generated METS content should match expected file exactly, including whitespace")
    }

    // Helper methods for XML parsing (using centralized XmlHelper)

    private fun parseXmlFile(file: File): Document = XmlHelper.parseXmlFile(file)

    private fun xpath(doc: Document, expression: String): String? = XmlHelper.xpath(doc, expression)

    private fun xpathNodeList(doc: Document, expression: String): NodeList = XmlHelper.xpathNodeList(doc, expression)

    private fun normalizeTimestampsInText(content: String): String {
        // Replace all timestamp patterns with a fixed placeholder for comparison
        return content
            .replace(Regex("""CREATEDATE="[^"]+""""), """CREATEDATE="NORMALIZED_TIMESTAMP"""")
            .replace(Regex("""LASTMODDATE="[^"]+""""), """LASTMODDATE="NORMALIZED_TIMESTAMP"""")
    }

    // ========== METS2 + MIX2 Tests ==========

    private val expectedMets2File = File("$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72-METS2_BROWSING.xml")

    @Test
    fun testSuccessfulMets2BrowsingGeneration() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(outputFile.exists(), "Output file should exist")
        assertTrue(outputFile.length() > 0, "Output file should not be empty")
    }

    @Test
    fun testMets2NamespacesAndSchema() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        val doc = parseXmlFile(outputFile)
        val content = outputFile.readText()

        // Verify METS v2 namespace
        assertTrue(content.contains("http://www.loc.gov/METS/v2"), "Should contain METS v2 namespace")

        // Verify MIX v2 namespace
        assertTrue(content.contains("http://www.loc.gov/mix/v20"), "Should contain MIX v2 namespace")

        // Verify root element
        assertEquals("mets", doc.documentElement.localName)
        assertEquals(objectId, doc.documentElement.getAttribute("OBJID"))
    }

    @Test
    fun testMets2MixStructure() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        val doc = parseXmlFile(outputFile)

        // Verify mdSec structure (METS v2 uses mdSec instead of amdSec)
        val mdSec = xpath(doc, "//*[local-name()='mdSec']")
        assertNotNull(mdSec, "mdSec should exist in METS v2")

        // Verify md elements (METS v2 uses md instead of techMD)
        val jpeg2000Encoding = xpath(doc, "//*[local-name()='md'][@ID='JPEG2000_ENCODING']")
        assertNotNull(jpeg2000Encoding, "JPEG2000_ENCODING md element should exist")

        // Verify resolution sections exist for all 4 pages
        val resolution1 = xpath(doc, "//*[local-name()='md'][@ID='RESOLUTION_P0001']")
        assertNotNull(resolution1, "RESOLUTION_P0001 should exist")

        val resolution4 = xpath(doc, "//*[local-name()='md'][@ID='RESOLUTION_P0004']")
        assertNotNull(resolution4, "RESOLUTION_P0004 should exist")
    }

    @Test
    fun testMets2Mix2ImageDimensions() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        val doc = parseXmlFile(outputFile)

        // Verify MIX 2.0 image dimensions structure
        val imageWidth = xpath(doc, "//*[@ID='RESOLUTION_P0001']//*[local-name()='imageWidth']/text()")
        assertNotNull(imageWidth, "Page 1 imageWidth should exist")
        assertTrue(imageWidth!!.toInt() > 0, "Page 1 imageWidth should be greater than 0")

        val imageHeight = xpath(doc, "//*[@ID='RESOLUTION_P0001']//*[local-name()='imageHeight']/text()")
        assertNotNull(imageHeight, "Page 1 imageHeight should exist")
        assertTrue(imageHeight!!.toInt() > 0, "Page 1 imageHeight should be greater than 0")
    }

    @Test
    fun testMets2Mix2SamplingFrequency() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        val doc = parseXmlFile(outputFile)

        // Verify MIX 2.0 uses string-based samplingFrequencyUnit (not integer like MIX 1.0)
        val samplingFrequencyUnit = xpath(doc, "//*[@ID='RESOLUTION_P0001']//*[local-name()='samplingFrequencyUnit']/text()")
        assertNotNull(samplingFrequencyUnit, "samplingFrequencyUnit should exist")
        // MIX 2.0 uses "cm" string instead of integer code
        assertTrue(samplingFrequencyUnit in listOf("in", "cm", "no absolute unit of measurement"),
            "samplingFrequencyUnit should be a valid MIX 2.0 string value, got: $samplingFrequencyUnit")

        // Verify sampling frequency numerator/denominator structure
        val xSamplingNumerator = xpath(doc, "//*[@ID='RESOLUTION_P0001']//*[local-name()='xSamplingFrequency']/*[local-name()='numerator']/text()")
        assertNotNull(xSamplingNumerator, "xSamplingFrequency numerator should exist")

        val xSamplingDenominator = xpath(doc, "//*[@ID='RESOLUTION_P0001']//*[local-name()='xSamplingFrequency']/*[local-name()='denominator']/text()")
        assertNotNull(xSamplingDenominator, "xSamplingFrequency denominator should exist")
    }

    @Test
    fun testMets2Jpeg2000EncodingOptions() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        val doc = parseXmlFile(outputFile)

        // Verify JPEG2000 encoding options in MIX 2.0 format
        val tileWidth = xpath(doc, "//*[@ID='JPEG2000_ENCODING']//*[local-name()='tileWidth']/text()")
        assertNotNull(tileWidth, "tileWidth should exist")

        val tileHeight = xpath(doc, "//*[@ID='JPEG2000_ENCODING']//*[local-name()='tileHeight']/text()")
        assertNotNull(tileHeight, "tileHeight should exist")

        val qualityLayers = xpath(doc, "//*[@ID='JPEG2000_ENCODING']//*[local-name()='qualityLayers']/text()")
        assertNotNull(qualityLayers, "qualityLayers should exist")

        val resolutionLevels = xpath(doc, "//*[@ID='JPEG2000_ENCODING']//*[local-name()='resolutionLevels']/text()")
        assertNotNull(resolutionLevels, "resolutionLevels should exist")
    }

    @Test
    fun testMets2FileSecAndStructMap() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        val doc = parseXmlFile(outputFile)

        // Verify fileSec has correct structure
        val fileGrp = xpath(doc, "//*[local-name()='fileGrp'][@ID='BROWSINGGRP']")
        assertNotNull(fileGrp, "BROWSINGGRP should exist")

        // Verify we have 4 DO entries (top-level only)
        val doEntries = xpathNodeList(doc, "//*[local-name()='fileGrp']/*[local-name()='file'][starts-with(@ID, 'DO_')]")
        assertEquals(4, doEntries.length, "Should have 4 DO entries")

        // Verify structMap has correct structure
        val structMap = xpath(doc, "//*[local-name()='structMap'][@TYPE='PHYSICAL']")
        assertNotNull(structMap, "PHYSICAL structMap should exist")

        // Verify we have 4 page divs
        val pageDivs = xpathNodeList(doc, "//*[local-name()='div'][@TYPE='PAGE']")
        assertEquals(4, pageDivs.length, "Should have 4 PAGE divs")
    }

    @Test
    fun testMets2ChecksumsAndFileSizes() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        val doc = parseXmlFile(outputFile)

        // Verify checksums exist (MD5 format)
        val alto1Checksum = xpath(doc, "//*[@ID='ALTO_0001']/@CHECKSUM")
        assertNotNull(alto1Checksum, "ALTO 0001 checksum should exist")
        assertTrue(alto1Checksum!!.length == 32, "ALTO 0001 checksum should be MD5 format")

        // Verify file sizes exist and are > 0
        val alto1Size = xpath(doc, "//*[@ID='ALTO_0001']/@SIZE")
        assertNotNull(alto1Size, "ALTO 0001 size should exist")
        assertTrue(alto1Size!!.toLong() > 0, "ALTO 0001 size should be greater than 0")

        val img1Checksum = xpath(doc, "//*[@ID='DO_0001_003']/@CHECKSUM")
        assertNotNull(img1Checksum, "Image 0001 (DO_0001_003) checksum should exist")
        assertTrue(img1Checksum!!.length == 32, "Image 0001 checksum should be MD5 format")
    }

    @Test
    fun testMets2MatchesExpectedOutput() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(outputFile.exists(), "Output file should exist")

        // Parse both files
        val generated = parseXmlFile(outputFile)
        val expected = parseXmlFile(expectedMets2File)

        // Compare key structure elements
        val generatedObjId = generated.documentElement.getAttribute("OBJID")
        val expectedObjId = expected.documentElement.getAttribute("OBJID")
        assertEquals(expectedObjId, generatedObjId, "OBJID should match")

        // Compare number of pages
        val generatedPages = xpathNodeList(generated, "//*[local-name()='div'][@TYPE='PAGE']")
        val expectedPages = xpathNodeList(expected, "//*[local-name()='div'][@TYPE='PAGE']")
        assertEquals(expectedPages.length, generatedPages.length, "Number of pages should match")

        // Compare image dimensions for page 1 using MIX 2.0 structure
        val generatedWidth1 = xpath(generated, "//*[@ID='RESOLUTION_P0001']//*[local-name()='imageWidth']/text()")
        val expectedWidth1 = xpath(expected, "//*[@ID='RESOLUTION_P0001']//*[local-name()='imageWidth']/text()")
        assertEquals(expectedWidth1, generatedWidth1, "Page 1 width should match")

        val generatedHeight1 = xpath(generated, "//*[@ID='RESOLUTION_P0001']//*[local-name()='imageHeight']/text()")
        val expectedHeight1 = xpath(expected, "//*[@ID='RESOLUTION_P0001']//*[local-name()='imageHeight']/text()")
        assertEquals(expectedHeight1, generatedHeight1, "Page 1 height should match")

        // Compare checksums
        val generatedAlto1Checksum = xpath(generated, "//*[@ID='ALTO_0001']/@CHECKSUM")
        val expectedAlto1Checksum = xpath(expected, "//*[@ID='ALTO_0001']/@CHECKSUM")
        assertEquals(expectedAlto1Checksum, generatedAlto1Checksum, "ALTO 1 checksum should match")

        val generatedImg1Checksum = xpath(generated, "//*[@ID='DO_0001_003']/@CHECKSUM")
        val expectedImg1Checksum = xpath(expected, "//*[@ID='DO_0001_003']/@CHECKSUM")
        assertEquals(expectedImg1Checksum, generatedImg1Checksum, "Image 1 checksum should match")
    }

    @Test
    fun testMets2PrettyPrintComparison() {
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(outputFile.exists(), "Output file should exist")

        // Read both files as text
        var generatedContent = outputFile.readText()
        var expectedContent = expectedMets2File.readText()

        // Normalize timestamps in both generated and expected content
        generatedContent = normalizeMets2Timestamps(generatedContent)
        expectedContent = normalizeMets2Timestamps(expectedContent)

        // Compare line by line for exact match including whitespace
        val generatedLines = generatedContent.lines()
        val expectedLines = expectedContent.lines()

        val differences = mutableListOf<String>()
        val maxLines = maxOf(expectedLines.size, generatedLines.size)

        for (i in 0 until maxLines) {
            val expectedLine = expectedLines.getOrNull(i) ?: ""
            val generatedLine = generatedLines.getOrNull(i) ?: ""

            if (expectedLine != generatedLine) {
                differences.add("Line ${i + 1}:")
                differences.add("  Expected:  '$expectedLine'")
                differences.add("  Generated: '$generatedLine'")
                differences.add("")
            }
        }

        if (differences.isNotEmpty()) {
            println("\n" + "=".repeat(80))
            println("METS2 TEXT DIFFERENCES FOUND (${differences.size / 4} lines differ)")
            println("=".repeat(80))
            differences.take(100).forEach { println(it) }
            if (differences.size > 100) {
                println("... and ${differences.size - 100} more difference lines")
            }
            println("=".repeat(80) + "\n")
        }

        assertEquals(expectedContent, generatedContent,
            "Generated METS2 content should match expected file exactly, including whitespace")
    }

    @Test
    fun testMets2DebugOutput() {
        val debugOutput = File("/tmp/debug_mets2_output.xml")

        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, debugOutput.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(debugOutput.exists(), "Output file should exist")

        println("Generated METS2 file saved to: ${debugOutput.absolutePath}")
        println("File size: ${debugOutput.length()} bytes")
    }

    @Test
    fun testDefaultVersionIsMets2WithMix2() {
        // Test that the default versions (when not explicitly set) produce valid METS2 + MIX2 output
        val runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        runner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, objectFolder)
        runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoFolderRelative)
        runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, imageFolderRelative)
        runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, jhoveFolderRelative)
        runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, outputFile.absolutePath)
        runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        // Not setting METS_VERSION and MIX_VERSION - should default to METS_2 and MIX_2_0

        runner.enqueue("test")
        runner.run()

        runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)

        val content = outputFile.readText()

        // Verify defaults are METS v2 and MIX v2
        assertTrue(content.contains("http://www.loc.gov/METS/v2"), "Default should be METS v2 namespace")
        assertTrue(content.contains("http://www.loc.gov/mix/v20"), "Default should be MIX v2 namespace")
    }

    private fun normalizeMets2Timestamps(content: String): String {
        // Replace all timestamp patterns with a fixed placeholder for comparison
        return content
            .replace(Regex("""CREATEDATE="[^"]+""""), """CREATEDATE="NORMALIZED_TIMESTAMP"""")
            .replace(Regex("""LASTMODDATE="[^"]+""""), """LASTMODDATE="NORMALIZED_TIMESTAMP"""")
    }

}
