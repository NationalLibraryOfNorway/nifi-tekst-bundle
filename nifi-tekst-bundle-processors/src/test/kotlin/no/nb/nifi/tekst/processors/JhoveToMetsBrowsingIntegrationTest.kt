package no.nb.nifi.tekst.processors

import no.nb.nifi.tekst.util.NiFiAttributes
import no.nb.nifi.tekst.util.XmlHelper
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.w3c.dom.Document
import org.w3c.dom.NodeList
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Integration test that chains the Jhove processor with the CreateMetsBrowsing processor.
 *
 * This test simulates a complete workflow:
 * 1. Start with tekst_<uuid>_no_jhove (no JHOVE metadata)
 * 2. Run Jhove processor on JP2 images to generate JHOVE metadata
 * 3. Run CreateMetsBrowsing processor using the generated JHOVE data
 * 4. Compare the result to the expected METS file from tekst_<uuid>
 */
class JhoveToMetsBrowsingIntegrationTest {

    private lateinit var tempJhoveOutputDir: File
    private lateinit var tempMetsOutputFile: File
    private val projectFolder = Paths.get("").toAbsolutePath().toString()

    // Source data without JHOVE
    private val noJhoveRoot = "$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_no_jhove"
    private val inputImagesPath = "$noJhoveRoot/access/data"
    private val altoPath = "$noJhoveRoot/access/metadata/other/ocr"

    // Expected output
    private val expectedMetsFile = File("$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72-METS_BROWSING.xml")

    // Configuration
    private val jhoveConfigPath = "$projectFolder/src/main/resources/jhoveconf.xml"
    // Object ID is derived from the folder name
    private val objectId = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_no_jhove"

    @BeforeEach
    fun setup() {
        tempJhoveOutputDir = Files.createTempDirectory("jhove_integration_test").toFile()
        tempMetsOutputFile = File.createTempFile("mets_integration_test", ".xml")
    }

    @AfterEach
    fun cleanup() {
        tempJhoveOutputDir.deleteRecursively()
        if (tempMetsOutputFile.exists()) {
            tempMetsOutputFile.delete()
        }
    }

    @Test
    fun testCompleteJhoveToMetsBrowsingWorkflow() {
        // Step 1: Run Jhove processor on all JP2 images
        val jhoveRunner = TestRunners.newTestRunner(Jhove::class.java)

        jhoveRunner.setProperty(Jhove.INPUT_PATH, inputImagesPath)
        jhoveRunner.setProperty(Jhove.OUTPUT_PATH, tempJhoveOutputDir.absolutePath)
        jhoveRunner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        jhoveRunner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        // Get all JP2 files from the input directory
        val imageFiles = File(inputImagesPath).listFiles { file ->
            file.extension == "jp2"
        }?.sortedBy { it.name } ?: emptyList()

        assertTrue(imageFiles.isNotEmpty(), "Should have JP2 images to process")
        assertEquals(4, imageFiles.size, "Should have exactly 4 test images")

        // Process each image through Jhove
        for (imageFile in imageFiles) {
            val attributes = HashMap<String, String>()
            attributes[NiFiAttributes.FILENAME] = imageFile.name

            jhoveRunner.enqueue("test", attributes)
        }

        jhoveRunner.run(imageFiles.size)

        // Verify Jhove processing succeeded
        jhoveRunner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, imageFiles.size)
        jhoveRunner.assertTransferCount(Jhove.JHOVE_OUTPUT_RELATIONSHIP, imageFiles.size)

        // Verify JHOVE output files were created
        val jhoveOutputFiles = tempJhoveOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }?.sortedBy { it.name }

        assertNotNull(jhoveOutputFiles, "JHOVE output files should exist")
        assertEquals(imageFiles.size, jhoveOutputFiles?.size, "Should have JHOVE output for each input image")

        // Verify JHOVE files contain valid data
        for (jhoveFile in jhoveOutputFiles!!) {
            assertTrue(jhoveFile.length() > 0, "JHOVE file ${jhoveFile.name} should not be empty")

            // Verify basic JHOVE structure
            val doc = parseXmlFile(jhoveFile)
            val status = xpath(doc, "//jhove:repInfo/jhove:status")
            assertNotNull(status, "JHOVE file ${jhoveFile.name} should have status")
            assertTrue(status!!.contains("Well-Formed"), "JHOVE status should indicate well-formed")

            // Verify MIX data is present (needed by CreateMetsBrowsing)
            val imageWidth = xpath(doc, "//*[local-name()='imageWidth']")
            val imageHeight = xpath(doc, "//*[local-name()='imageHeight']")
            assertNotNull(imageWidth, "JHOVE file ${jhoveFile.name} should have imageWidth")
            assertNotNull(imageHeight, "JHOVE file ${jhoveFile.name} should have imageHeight")
            assertTrue(imageWidth!!.toInt() > 0, "imageWidth should be positive")
            assertTrue(imageHeight!!.toInt() > 0, "imageHeight should be positive")
        }

        println("✓ Step 1 complete: Generated ${jhoveOutputFiles.size} JHOVE metadata files")

        // Step 2: Run CreateMetsBrowsing processor using generated JHOVE data
        val metsRunner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        metsRunner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, noJhoveRoot)
        metsRunner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoPath)
        metsRunner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, inputImagesPath)
        metsRunner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, tempJhoveOutputDir.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, tempMetsOutputFile.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        metsRunner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        metsRunner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        metsRunner.enqueue("test")
        metsRunner.run()

        // Verify METS generation succeeded
        metsRunner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(tempMetsOutputFile.exists(), "METS output file should exist")
        assertTrue(tempMetsOutputFile.length() > 0, "METS output file should not be empty")

        println("✓ Step 2 complete: Generated METS-browsing file (${tempMetsOutputFile.length()} bytes)")

        // Step 3: Compare generated METS with expected output
        val generatedDoc = parseXmlFile(tempMetsOutputFile)
        val expectedDoc = parseXmlFile(expectedMetsFile)

        // Verify basic METS structure
        assertEquals("mets", generatedDoc.documentElement.localName, "Root element should be 'mets'")
        assertEquals(objectId, generatedDoc.documentElement.getAttribute("OBJID"), "OBJID should match")

        // Verify number of pages matches
        val generatedPages = xpathNodeList(generatedDoc, "//*[local-name()='div'][@TYPE='PAGE']")
        val expectedPages = xpathNodeList(expectedDoc, "//*[local-name()='div'][@TYPE='PAGE']")
        assertEquals(expectedPages.length, generatedPages.length, "Number of pages should match expected output")
        assertEquals(4, generatedPages.length, "Should have 4 pages")

        // Verify image dimensions match for all pages
        for (pageNum in 1..4) {
            val pageId = String.format("%04d", pageNum)

            val generatedWidth = xpath(generatedDoc, "//*[@ID='TM_$pageId']//*[local-name()='imageWidth']/text()")
            val expectedWidth = xpath(expectedDoc, "//*[@ID='TM_$pageId']//*[local-name()='imageWidth']/text()")
            assertEquals(expectedWidth, generatedWidth, "Page $pageNum width should match expected")

            val generatedHeight = xpath(generatedDoc, "//*[@ID='TM_$pageId']//*[local-name()='imageHeight']/text()")
            val expectedHeight = xpath(expectedDoc, "//*[@ID='TM_$pageId']//*[local-name()='imageHeight']/text()")
            assertEquals(expectedHeight, generatedHeight, "Page $pageNum height should match expected")
        }

        // Verify checksums match (since we're using the same source files)
        for (fileNum in 1..4) {
            val fileId = String.format("%04d", fileNum)

            val generatedAltoChecksum = xpath(generatedDoc, "//*[@ID='ALTO_$fileId']/@CHECKSUM")
            val expectedAltoChecksum = xpath(expectedDoc, "//*[@ID='ALTO_$fileId']/@CHECKSUM")
            assertEquals(expectedAltoChecksum, generatedAltoChecksum, "ALTO $fileNum checksum should match")

            val generatedImgChecksum = xpath(generatedDoc, "//*[@ID='IMG_$fileId']/@CHECKSUM")
            val expectedImgChecksum = xpath(expectedDoc, "//*[@ID='IMG_$fileId']/@CHECKSUM")
            assertEquals(expectedImgChecksum, generatedImgChecksum, "Image $fileNum checksum should match")
        }

        // Verify file sizes match
        for (fileNum in 1..4) {
            val fileId = String.format("%04d", fileNum)

            val generatedAltoSize = xpath(generatedDoc, "//*[@ID='ALTO_$fileId']/@SIZE")
            val expectedAltoSize = xpath(expectedDoc, "//*[@ID='ALTO_$fileId']/@SIZE")
            assertEquals(expectedAltoSize, generatedAltoSize, "ALTO $fileNum size should match")

            val generatedImgSize = xpath(generatedDoc, "//*[@ID='IMG_$fileId']/@SIZE")
            val expectedImgSize = xpath(expectedDoc, "//*[@ID='IMG_$fileId']/@SIZE")
            assertEquals(expectedImgSize, generatedImgSize, "Image $fileNum size should match")
        }

        // Verify URNs are correctly formatted
        for (fileNum in 1..4) {
            val fileId = String.format("%04d", fileNum)

            val generatedAltoUrn = xpath(generatedDoc, "//*[@ID='ALTO_$fileId']/*[@LOCTYPE='URN']/@*[local-name()='href']")
            val expectedAltoUrn = xpath(expectedDoc, "//*[@ID='ALTO_$fileId']/*[@LOCTYPE='URN']/@*[local-name()='href']")
            assertEquals(expectedAltoUrn, generatedAltoUrn, "ALTO $fileNum URN should match")

            val generatedImgUrn = xpath(generatedDoc, "//*[@ID='IMG_$fileId']/*[@LOCTYPE='URN']/@*[local-name()='href']")
            val expectedImgUrn = xpath(expectedDoc, "//*[@ID='IMG_$fileId']/*[@LOCTYPE='URN']/@*[local-name()='href']")
            assertEquals(expectedImgUrn, generatedImgUrn, "Image $fileNum URN should match")
        }

        // Verify file URLs are correct
        for (fileNum in 1..4) {
            val fileId = String.format("%04d", fileNum)

            val generatedAltoUrl = xpath(generatedDoc, "//*[@ID='ALTO_$fileId']/*[@LOCTYPE='URL']/@*[local-name()='href']")
            val expectedAltoUrl = xpath(expectedDoc, "//*[@ID='ALTO_$fileId']/*[@LOCTYPE='URL']/@*[local-name()='href']")
            assertEquals(expectedAltoUrl, generatedAltoUrl, "ALTO $fileNum URL should match")

            val generatedImgUrl = xpath(generatedDoc, "//*[@ID='IMG_$fileId']/*[@LOCTYPE='URL']/@*[local-name()='href']")
            val expectedImgUrl = xpath(expectedDoc, "//*[@ID='IMG_$fileId']/*[@LOCTYPE='URL']/@*[local-name()='href']")
            assertEquals(expectedImgUrl, generatedImgUrl, "Image $fileNum URL should match")
        }

        println("✓ Step 3 complete: Generated METS matches expected output")
        println("✓ Integration test passed: Jhove → CreateMetsBrowsing workflow successful")
    }

    @Test
    fun testIntegrationWithMissingJhoveFile() {
        // Step 1: Run Jhove processor on only SOME images (not all)
        val jhoveRunner = TestRunners.newTestRunner(Jhove::class.java)

        jhoveRunner.setProperty(Jhove.INPUT_PATH, inputImagesPath)
        jhoveRunner.setProperty(Jhove.OUTPUT_PATH, tempJhoveOutputDir.absolutePath)
        jhoveRunner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        jhoveRunner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        // Process only the first 3 images (skip the 4th)
        val imageFiles = File(inputImagesPath).listFiles { file ->
            file.extension == "jp2"
        }?.sortedBy { it.name }?.take(3) ?: emptyList()

        for (imageFile in imageFiles) {
            val attributes = HashMap<String, String>()
            attributes[NiFiAttributes.FILENAME] = imageFile.name
            jhoveRunner.enqueue("test", attributes)
        }

        jhoveRunner.run(imageFiles.size)

        // Verify only 3 JHOVE files were created
        val jhoveFiles = tempJhoveOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }
        assertEquals(3, jhoveFiles?.size, "Should have only 3 JHOVE files")

        // Step 2: Run CreateMetsBrowsing - should FAIL with clear error message
        val metsRunner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        metsRunner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, noJhoveRoot)
        metsRunner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoPath)
        metsRunner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, inputImagesPath)
        metsRunner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, tempJhoveOutputDir.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, tempMetsOutputFile.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        metsRunner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        metsRunner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        metsRunner.enqueue("test")
        metsRunner.run()

        // Should fail because JHOVE file for 4th image is missing
        metsRunner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_FAILURE)

        // Verify the METS file was not created or is incomplete
        assertFalse(tempMetsOutputFile.exists() && tempMetsOutputFile.length() > 1000,
            "METS output file should not be successfully created when JHOVE files are missing")

        println("✓ CreateMetsBrowsing correctly failed when JHOVE file was missing")
    }

    @Test
    fun testIntegrationPerformanceMetrics() {
        val startTime = System.currentTimeMillis()

        // Run Jhove processor
        val jhoveRunner = TestRunners.newTestRunner(Jhove::class.java)
        jhoveRunner.setProperty(Jhove.INPUT_PATH, inputImagesPath)
        jhoveRunner.setProperty(Jhove.OUTPUT_PATH, tempJhoveOutputDir.absolutePath)
        jhoveRunner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        jhoveRunner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val imageFiles = File(inputImagesPath).listFiles { file ->
            file.extension == "jp2"
        }?.sortedBy { it.name } ?: emptyList()

        for (imageFile in imageFiles) {
            val attributes = HashMap<String, String>()
            attributes[NiFiAttributes.FILENAME] = imageFile.name
            jhoveRunner.enqueue("test", attributes)
        }

        val jhoveStartTime = System.currentTimeMillis()
        jhoveRunner.run(imageFiles.size)
        val jhoveEndTime = System.currentTimeMillis()

        // Run CreateMetsBrowsing processor
        val metsRunner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)
        metsRunner.setProperty(CreateMetsBrowsing.OBJECT_FOLDER, noJhoveRoot)
        metsRunner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoPath)
        metsRunner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, inputImagesPath)
        metsRunner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, tempJhoveOutputDir.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, tempMetsOutputFile.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
        metsRunner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
        metsRunner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

        val metsStartTime = System.currentTimeMillis()
        metsRunner.enqueue("test")
        metsRunner.run()
        val metsEndTime = System.currentTimeMillis()

        val totalTime = System.currentTimeMillis() - startTime

        println("=== Integration Test Performance Metrics ===")
        println("Jhove processing time: ${jhoveEndTime - jhoveStartTime}ms for ${imageFiles.size} images")
        println("METS generation time: ${metsEndTime - metsStartTime}ms")
        println("Total workflow time: ${totalTime}ms")
        println("Average time per image: ${totalTime / imageFiles.size}ms")

        // Basic assertion that the workflow completes in reasonable time
        assertTrue(totalTime < 120000, "Total workflow should complete in under 2 minutes")
    }

    // Helper methods for XML parsing (using centralized XmlHelper)

    private fun parseXmlFile(file: File): Document = XmlHelper.parseXmlFile(file)

    private fun xpath(doc: Document, expression: String): String? = XmlHelper.xpath(doc, expression)

    private fun xpathNodeList(doc: Document, expression: String): NodeList = XmlHelper.xpathNodeList(doc, expression)
}
