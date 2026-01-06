package no.nb.nifi.tekst.processors

import no.nb.nifi.tekst.util.NiFiAttributes
import no.nb.nifi.tekst.util.XmlHelper
import no.nb.nifi.tekst.validation.XsdValidator
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
 * Integration test that chains the Jhove processor with the CreateMetsBrowsing processor
 * to generate METS2 with MIX2 metadata.
 *
 * This test simulates a complete workflow:
 * 1. Start with tekst_<uuid>_no_jhove (no JHOVE metadata)
 * 2. Run Jhove processor on JP2 images to generate JHOVE metadata
 * 3. Run CreateMetsBrowsing processor using the generated JHOVE data with METS_2 and MIX_2_0 versions
 * 4. Validate the generated METS2 XML against XSD schemas
 * 5. Compare the result to the expected METS2 file from tekst_<uuid>
 */
class JhoveToMets2BrowsingIntegrationTest {

    private lateinit var tempJhoveOutputDir: File
    private lateinit var tempMetsOutputFile: File
    private val projectFolder = Paths.get("").toAbsolutePath().toString()

    // Source data without JHOVE
    private val noJhoveRoot = "$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_no_jhove"
    private val inputImagesPath = "$noJhoveRoot/access/data"
    private val altoPath = "$noJhoveRoot/access/metadata/other/ocr"

    // Expected output - METS2 version
    private val expectedMets2File = File("$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72-METS2_BROWSING.xml")

    // Configuration
    private val objectId = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72"

    @BeforeEach
    fun setup() {
        tempJhoveOutputDir = Files.createTempDirectory("jhove_mets2_integration_test").toFile()
        tempMetsOutputFile = File.createTempFile("mets2_integration_test", ".xml")
    }

    @AfterEach
    fun cleanup() {
        tempJhoveOutputDir.deleteRecursively()
        if (tempMetsOutputFile.exists()) {
            tempMetsOutputFile.delete()
        }
    }

    @Test
    fun testCompleteJhoveToMets2BrowsingWorkflowWithXsdValidation() {
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

        // Verify JHOVE files contain valid data and validate against JHOVE XSD
        for (jhoveFile in jhoveOutputFiles!!) {
            assertTrue(jhoveFile.length() > 0, "JHOVE file ${jhoveFile.name} should not be empty")

            // Validate JHOVE XML against XSD
            val jhoveValidationResult = XsdValidator.validateJhove(jhoveFile)
            if (!jhoveValidationResult.isValid) {
                println("JHOVE XSD validation errors for ${jhoveFile.name}:")
                jhoveValidationResult.errors.forEach { println("  - $it") }
            }
            assertTrue(jhoveValidationResult.isValid, "JHOVE file ${jhoveFile.name} should be valid against JHOVE XSD")

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

        println("✓ Step 1 complete: Generated ${jhoveOutputFiles.size} JHOVE metadata files with XSD validation")

        // Step 2: Run CreateMetsBrowsing processor using generated JHOVE data with METS2 and MIX2
        val metsRunner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        metsRunner.setProperty(CreateMetsBrowsing.OBJECT_ID, objectId)
        metsRunner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoPath)
        metsRunner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, inputImagesPath)
        metsRunner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, tempJhoveOutputDir.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, tempMetsOutputFile.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        metsRunner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        metsRunner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        metsRunner.enqueue("test")
        metsRunner.run()

        // Verify METS generation succeeded
        metsRunner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)
        assertTrue(tempMetsOutputFile.exists(), "METS2 output file should exist")
        assertTrue(tempMetsOutputFile.length() > 0, "METS2 output file should not be empty")

        println("✓ Step 2 complete: Generated METS2-browsing file (${tempMetsOutputFile.length()} bytes)")

        // Step 3: Validate generated METS2 XML against XSD
        val generatedMetsContent = tempMetsOutputFile.readText()
        val mets2ValidationResult = XsdValidator.validateMets2(generatedMetsContent)

        if (!mets2ValidationResult.isValid) {
            println("Generated METS2 XML:")
            println(generatedMetsContent.take(2000))
            println("\nMETS2 XSD validation errors:")
            mets2ValidationResult.errors.forEach { println("  - $it") }
        }
        assertTrue(mets2ValidationResult.isValid, "Generated METS2 should be valid against METS2 XSD")

        println("✓ Step 3 complete: Generated METS2 passed XSD validation")

        // Step 3b: Validate MIX2 data embedded in METS2 against MIX2 XSD
        val mix2ValidationResult = XsdValidator.validateMix2InMets(generatedMetsContent)

        if (!mix2ValidationResult.isValid) {
            println("MIX2 XSD validation errors:")
            mix2ValidationResult.errors.forEach { println("  - $it") }
        }
        assertTrue(mix2ValidationResult.isValid, "Embedded MIX2 data should be valid against MIX2 XSD")

        println("✓ Step 3b complete: Embedded MIX2 data passed XSD validation")

        // Step 4: Compare generated METS2 with expected output
        val generatedDoc = parseXmlFile(tempMetsOutputFile)
        val expectedDoc = parseXmlFile(expectedMets2File)

        // Verify basic METS2 structure
        assertEquals("mets", generatedDoc.documentElement.localName, "Root element should be 'mets'")
        assertEquals(objectId, generatedDoc.documentElement.getAttribute("OBJID"), "OBJID should match")

        // Verify METS2 namespace
        assertTrue(generatedMetsContent.contains("http://www.loc.gov/METS/v2"),
            "Generated METS should use METS v2 namespace")

        // Verify MIX2 namespace
        assertTrue(generatedMetsContent.contains("http://www.loc.gov/mix/v20"),
            "Generated METS should use MIX v2.0 namespace")

        // Verify METS2-specific elements
        assertTrue(generatedMetsContent.contains("mdSec"),
            "METS2 should use mdSec element")
        assertTrue(generatedMetsContent.contains("structSec"),
            "METS2 should wrap structMap in structSec")
        assertTrue(generatedMetsContent.contains("LOCREF"),
            "METS2 should use LOCREF instead of xlink:href")

        // Verify number of pages matches
        val generatedPages = xpathNodeList(generatedDoc, "//*[local-name()='div'][@TYPE='PAGE']")
        val expectedPages = xpathNodeList(expectedDoc, "//*[local-name()='div'][@TYPE='PAGE']")
        assertEquals(expectedPages.length, generatedPages.length, "Number of pages should match expected output")
        assertEquals(4, generatedPages.length, "Should have 4 pages")

        // Verify image dimensions match for all pages (MIX2 format)
        for (pageNum in 1..4) {
            val pageId = String.format("%04d", pageNum)

            val generatedWidth = xpath(generatedDoc, "//*[@ID='RESOLUTION_P$pageId']//*[local-name()='imageWidth']/text()")
            val expectedWidth = xpath(expectedDoc, "//*[@ID='RESOLUTION_P$pageId']//*[local-name()='imageWidth']/text()")
            assertEquals(expectedWidth, generatedWidth, "Page $pageNum width should match expected")

            val generatedHeight = xpath(generatedDoc, "//*[@ID='RESOLUTION_P$pageId']//*[local-name()='imageHeight']/text()")
            val expectedHeight = xpath(expectedDoc, "//*[@ID='RESOLUTION_P$pageId']//*[local-name()='imageHeight']/text()")
            assertEquals(expectedHeight, generatedHeight, "Page $pageNum height should match expected")
        }

        // Verify MIX2-specific elements (samplingFrequencyUnit as string)
        val samplingFrequencyUnits = xpath(generatedDoc, "//*[local-name()='samplingFrequencyUnit']/text()")
        assertNotNull(samplingFrequencyUnits, "Should have samplingFrequencyUnit in MIX2")
        // MIX v2.0 uses string values like "cm" instead of integer codes
        assertTrue(samplingFrequencyUnits == "cm" || samplingFrequencyUnits == "inch",
            "MIX v2.0 samplingFrequencyUnit should be a string value ('cm' or 'inch')")

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

        // Verify URNs are correctly formatted (METS2 uses LOCREF)
        for (fileNum in 1..4) {
            val fileId = String.format("%04d", fileNum)

            val generatedAltoUrn = xpath(generatedDoc, "//*[@ID='ALTO_$fileId']/*[@LOCTYPE='URN']/@LOCREF")
            val expectedAltoUrn = xpath(expectedDoc, "//*[@ID='ALTO_$fileId']/*[@LOCTYPE='URN']/@LOCREF")
            assertEquals(expectedAltoUrn, generatedAltoUrn, "ALTO $fileNum URN should match")

            val generatedImgUrn = xpath(generatedDoc, "//*[@ID='IMG_$fileId']/*[@LOCTYPE='URN']/@LOCREF")
            val expectedImgUrn = xpath(expectedDoc, "//*[@ID='IMG_$fileId']/*[@LOCTYPE='URN']/@LOCREF")
            assertEquals(expectedImgUrn, generatedImgUrn, "Image $fileNum URN should match")
        }

        println("✓ Step 4 complete: Generated METS2 matches expected output")
        println("✓ Integration test passed: Jhove → CreateMetsBrowsing (METS2 + MIX2) workflow successful with XSD validation")
    }

    @Test
    fun testMets2GenerationFailsValidationWithMissingJhoveFile() {
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

        // Step 2: Run CreateMetsBrowsing with METS2 - should FAIL with clear error message
        val metsRunner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)

        metsRunner.setProperty(CreateMetsBrowsing.OBJECT_ID, objectId)
        metsRunner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoPath)
        metsRunner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, inputImagesPath)
        metsRunner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, tempJhoveOutputDir.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, tempMetsOutputFile.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        metsRunner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        metsRunner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        metsRunner.enqueue("test")
        metsRunner.run()

        // Should fail because JHOVE file for 4th image is missing
        metsRunner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_FAILURE)

        // Verify the METS file was not created or is incomplete
        assertFalse(tempMetsOutputFile.exists() && tempMetsOutputFile.length() > 1000,
            "METS2 output file should not be successfully created when JHOVE files are missing")

        println("✓ CreateMetsBrowsing (METS2) correctly failed when JHOVE file was missing")
    }

    @Test
    fun testMets2VsMets1StructuralDifferences() {
        // Step 1: Generate JHOVE metadata
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

        jhoveRunner.run(imageFiles.size)

        // Step 2: Generate METS1 with MIX1
        val mets1OutputFile = File.createTempFile("mets1_comparison", ".xml")
        try {
            val mets1Runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)
            mets1Runner.setProperty(CreateMetsBrowsing.OBJECT_ID, objectId)
            mets1Runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoPath)
            mets1Runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, inputImagesPath)
            mets1Runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, tempJhoveOutputDir.absolutePath)
            mets1Runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, mets1OutputFile.absolutePath)
            mets1Runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v1")
            mets1Runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_1")
            mets1Runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_1_0")

            mets1Runner.enqueue("test")
            mets1Runner.run()
            mets1Runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)

            // Step 3: Generate METS2 with MIX2
            val mets2Runner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)
            mets2Runner.setProperty(CreateMetsBrowsing.OBJECT_ID, objectId)
            mets2Runner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoPath)
            mets2Runner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, inputImagesPath)
            mets2Runner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, tempJhoveOutputDir.absolutePath)
            mets2Runner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, tempMetsOutputFile.absolutePath)
            mets2Runner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
            mets2Runner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
            mets2Runner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

            mets2Runner.enqueue("test")
            mets2Runner.run()
            mets2Runner.assertAllFlowFilesTransferred(CreateMetsBrowsing.REL_SUCCESS)

            // Step 4: Validate both against respective XSDs
            val mets1Content = mets1OutputFile.readText()
            val mets2Content = tempMetsOutputFile.readText()

            val mets1Validation = XsdValidator.validateMets(mets1Content)
            val mets2Validation = XsdValidator.validateMets2(mets2Content)

            assertTrue(mets1Validation.isValid, "METS1 should be valid against METS1 XSD: ${mets1Validation.errors}")
            assertTrue(mets2Validation.isValid, "METS2 should be valid against METS2 XSD: ${mets2Validation.errors}")

            // Step 4b: Validate embedded MIX data against respective MIX XSDs
            val mix1Validation = XsdValidator.validateMixInMets(mets1Content)
            val mix2Validation = XsdValidator.validateMix2InMets(mets2Content)

            assertTrue(mix1Validation.isValid, "METS1 embedded MIX1 should be valid: ${mix1Validation.errors}")
            assertTrue(mix2Validation.isValid, "METS2 embedded MIX2 should be valid: ${mix2Validation.errors}")

            // Step 5: Verify structural differences
            // METS1 characteristics
            assertTrue(mets1Content.contains("http://www.loc.gov/METS/\""),
                "METS1 should have v1 namespace (ending with /METS/\")")
            assertTrue(mets1Content.contains("amdSec"),
                "METS1 should use amdSec element")
            assertTrue(mets1Content.contains("xlink:href"),
                "METS1 should use xlink:href for references")
            assertTrue(mets1Content.contains("http://www.loc.gov/mix/v10"),
                "METS1 with MIX1 should use MIX v1.0 namespace")
            assertTrue(mets1Content.contains("<mix:samplingFrequencyUnit>3</mix:samplingFrequencyUnit>"),
                "MIX v1.0 should use integer samplingFrequencyUnit")

            // METS2 characteristics
            assertTrue(mets2Content.contains("http://www.loc.gov/METS/v2"),
                "METS2 should have v2 namespace")
            assertTrue(mets2Content.contains("mdSec"),
                "METS2 should use mdSec element")
            assertTrue(mets2Content.contains("LOCREF"),
                "METS2 should use LOCREF for references")
            assertTrue(mets2Content.contains("structSec"),
                "METS2 should wrap structMap in structSec")
            assertTrue(mets2Content.contains("http://www.loc.gov/mix/v20"),
                "METS2 with MIX2 should use MIX v2.0 namespace")
            assertTrue(mets2Content.contains("<mix:samplingFrequencyUnit>cm</mix:samplingFrequencyUnit>"),
                "MIX v2.0 should use string samplingFrequencyUnit")

            // Both should have same content data (checksums, sizes, URNs)
            val mets1Doc = parseXmlFile(mets1OutputFile)
            val mets2Doc = parseXmlFile(tempMetsOutputFile)

            for (fileNum in 1..4) {
                val fileId = String.format("%04d", fileNum)

                // Compare checksums
                val mets1ImgChecksum = xpath(mets1Doc, "//*[@ID='IMG_$fileId']/@CHECKSUM")
                val mets2ImgChecksum = xpath(mets2Doc, "//*[@ID='IMG_$fileId']/@CHECKSUM")
                assertEquals(mets1ImgChecksum, mets2ImgChecksum,
                    "Image $fileNum checksum should be same in both METS versions")

                // Compare sizes
                val mets1ImgSize = xpath(mets1Doc, "//*[@ID='IMG_$fileId']/@SIZE")
                val mets2ImgSize = xpath(mets2Doc, "//*[@ID='IMG_$fileId']/@SIZE")
                assertEquals(mets1ImgSize, mets2ImgSize,
                    "Image $fileNum size should be same in both METS versions")
            }

            println("✓ METS1 vs METS2 structural differences verified")
            println("✓ Both versions contain same content data with different structure")

        } finally {
            mets1OutputFile.delete()
        }
    }

    @Test
    fun testMets2Mix2IntegrationPerformanceMetrics() {
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

        // Verify JHOVE files against XSD
        val jhoveOutputFiles = tempJhoveOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }?.sortedBy { it.name } ?: emptyList()

        val xsdValidationStartTime = System.currentTimeMillis()
        for (jhoveFile in jhoveOutputFiles) {
            val result = XsdValidator.validateJhove(jhoveFile)
            assertTrue(result.isValid, "JHOVE file ${jhoveFile.name} should be valid")
        }
        val xsdValidationEndTime = System.currentTimeMillis()

        // Run CreateMetsBrowsing processor with METS2 + MIX2
        val metsRunner = TestRunners.newTestRunner(CreateMetsBrowsing::class.java)
        metsRunner.setProperty(CreateMetsBrowsing.OBJECT_ID, objectId)
        metsRunner.setProperty(CreateMetsBrowsing.ALTO_FOLDER, altoPath)
        metsRunner.setProperty(CreateMetsBrowsing.IMAGE_FOLDER, inputImagesPath)
        metsRunner.setProperty(CreateMetsBrowsing.JHOVE_FOLDER, tempJhoveOutputDir.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.OUTPUT_FILE, tempMetsOutputFile.absolutePath)
        metsRunner.setProperty(CreateMetsBrowsing.AGENT_NAME, "MetsBrowsingGenerator v2")
        metsRunner.setProperty(CreateMetsBrowsing.METS_VERSION, "METS_2")
        metsRunner.setProperty(CreateMetsBrowsing.MIX_VERSION, "MIX_2_0")

        val metsStartTime = System.currentTimeMillis()
        metsRunner.enqueue("test")
        metsRunner.run()
        val metsEndTime = System.currentTimeMillis()

        // Validate generated METS2 against XSD
        val mets2ValidationStartTime = System.currentTimeMillis()
        val mets2Content = tempMetsOutputFile.readText()
        val mets2ValidationResult = XsdValidator.validateMets2(mets2Content)
        assertTrue(mets2ValidationResult.isValid, "Generated METS2 should be valid against XSD")
        val mets2ValidationEndTime = System.currentTimeMillis()

        // Validate embedded MIX2 data against XSD
        val mix2ValidationStartTime = System.currentTimeMillis()
        val mix2ValidationResult = XsdValidator.validateMix2InMets(mets2Content)
        assertTrue(mix2ValidationResult.isValid, "Embedded MIX2 should be valid against XSD")
        val mix2ValidationEndTime = System.currentTimeMillis()

        val totalTime = System.currentTimeMillis() - startTime

        println("=== METS2 + MIX2 Integration Test Performance Metrics ===")
        println("Jhove processing time: ${jhoveEndTime - jhoveStartTime}ms for ${imageFiles.size} images")
        println("JHOVE XSD validation time: ${xsdValidationEndTime - xsdValidationStartTime}ms for ${jhoveOutputFiles.size} files")
        println("METS2 generation time: ${metsEndTime - metsStartTime}ms")
        println("METS2 XSD validation time: ${mets2ValidationEndTime - mets2ValidationStartTime}ms")
        println("MIX2 XSD validation time: ${mix2ValidationEndTime - mix2ValidationStartTime}ms")
        println("Total workflow time: ${totalTime}ms")
        println("Average time per image: ${totalTime / imageFiles.size}ms")

        // Basic assertion that the workflow completes in reasonable time
        assertTrue(totalTime < 120000, "Total workflow should complete in under 2 minutes")
    }

    @Test
    fun testMets2Mix2ValidatesAgainstExpectedFile() {
        // This test validates that the expected METS2_BROWSING.xml file is valid against XSD
        assertTrue(expectedMets2File.exists(), "Expected METS2 file should exist: ${expectedMets2File.absolutePath}")

        val result = XsdValidator.validateMets2(expectedMets2File)

        if (!result.isValid) {
            println("Expected METS2 file validation errors:")
            result.errors.forEach { println("  - $it") }
        }

        assertTrue(result.isValid, "Expected METS2_BROWSING.xml should be valid against METS2 XSD")

        // Validate embedded MIX2 data against MIX2 XSD
        val content = expectedMets2File.readText()
        val mix2Result = XsdValidator.validateMix2InMets(content)
        if (!mix2Result.isValid) {
            println("MIX2 validation errors in expected file:")
            mix2Result.errors.forEach { println("  - $it") }
        }
        assertTrue(mix2Result.isValid, "Expected METS2_BROWSING.xml should have valid MIX2 data")

        // Verify it contains METS2 and MIX2 namespaces
        assertTrue(content.contains("http://www.loc.gov/METS/v2"),
            "Expected file should use METS v2 namespace")
        assertTrue(content.contains("http://www.loc.gov/mix/v20"),
            "Expected file should use MIX v2.0 namespace")

        println("✓ Expected METS2_BROWSING.xml is valid against METS2 XSD")
    }

    // Helper methods for XML parsing (using centralized XmlHelper)

    private fun parseXmlFile(file: File): Document = XmlHelper.parseXmlFile(file)

    private fun xpath(doc: Document, expression: String): String? = XmlHelper.xpath(doc, expression)

    private fun xpathNodeList(doc: Document, expression: String): NodeList = XmlHelper.xpathNodeList(doc, expression)
}

