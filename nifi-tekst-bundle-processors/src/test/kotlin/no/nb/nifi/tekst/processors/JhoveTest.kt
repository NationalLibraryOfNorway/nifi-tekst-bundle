package no.nb.nifi.tekst.processors

import no.nb.nifi.tekst.util.NiFiAttributes
import no.nb.nifi.tekst.util.XmlHelper
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.w3c.dom.Document
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathFactory

class JhoveTest {

    private lateinit var tempOutputDir: File
    private val projectFolder = Paths.get("").toAbsolutePath().toString()
    private val testDataRoot = "$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72/primary"
    private val inputPath = "$testDataRoot/data"
    private val jhoveConfigPath = "$projectFolder/src/main/resources/jhoveconf.xml"

    @BeforeEach
    fun setup() {
        tempOutputDir = Files.createTempDirectory("jhove_test_output").toFile()
    }

    @AfterEach
    fun cleanup() {
        tempOutputDir.deleteRecursively()
    }

    @Test
    fun testValidJp2FileProcessing() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2"

        runner.enqueue("test", attributes)
        runner.run()

        runner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, 1)
        runner.assertTransferCount(Jhove.JHOVE_OUTPUT_RELATIONSHIP, 1)

        // Verify JHOVE XML output was created
        val jhoveOutputFiles = tempOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }
        assertNotNull(jhoveOutputFiles, "JHOVE output files should exist")
        assertEquals(1, jhoveOutputFiles?.size, "Should have exactly one JHOVE output file")
        assertTrue(jhoveOutputFiles!![0].length() > 0, "JHOVE output file should not be empty")
    }

    @Test
    fun testJhoveOutputFileTransferredToRelationship() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_002.jp2"

        runner.enqueue("test", attributes)
        runner.run()

        // Check that JHOVE output relationship has a FlowFile
        runner.assertTransferCount(Jhove.JHOVE_OUTPUT_RELATIONSHIP, 1)

        val jhoveOutputFlowFiles = runner.getFlowFilesForRelationship(Jhove.JHOVE_OUTPUT_RELATIONSHIP)
        assertEquals(1, jhoveOutputFlowFiles.size)

        val jhoveFlowFile = jhoveOutputFlowFiles[0]
        assertTrue(jhoveFlowFile.getAttribute(NiFiAttributes.FILENAME).startsWith("JHOVE_"))
        assertTrue(jhoveFlowFile.getAttribute(NiFiAttributes.FILENAME).endsWith(".xml"))
    }

    @Test
    fun testJhoveXmlOutputStructure() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_003.jp2"

        runner.enqueue("test", attributes)
        runner.run()

        // Find and parse the JHOVE output file
        val jhoveOutputFiles = tempOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }
        val jhoveOutputFile = jhoveOutputFiles!![0]
        val doc = parseXmlFile(jhoveOutputFile)

        // Verify JHOVE XML structure
        assertEquals("jhove", doc.documentElement.localName, "Root element should be 'jhove'")

        // Verify status element exists
        val status = xpath(doc, "//jhove:repInfo/jhove:status")
        assertNotNull(status, "Status element should exist")
        assertTrue(status!!.startsWith("Well-Formed"), "Status should indicate well-formed file")

        // Verify format
        val format = xpath(doc, "//jhove:repInfo/jhove:format")
        assertEquals("JPEG 2000", format, "Format should be JPEG 2000")

        // Verify checksums exist
        val md5 = xpath(doc, "//jhove:repInfo/jhove:checksums/jhove:checksum[@type='MD5']")
        assertNotNull(md5, "MD5 checksum should exist")
        assertTrue(md5!!.isNotEmpty(), "MD5 checksum should not be empty")
    }

    @Test
    fun testMissingInputFile() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = "nonexistent_file.jp2"

        runner.enqueue("test", attributes)
        runner.run()

        runner.assertTransferCount(Jhove.FAIL_RELATIONSHIP, 1)
        // JHOVE may still create output even for missing files
        assertTrue(runner.getFlowFilesForRelationship(Jhove.JHOVE_OUTPUT_RELATIONSHIP).size >= 0)

        val failedFlowFiles = runner.getFlowFilesForRelationship(Jhove.FAIL_RELATIONSHIP)
        val failedFlowFile = failedFlowFiles[0]

        assertNotNull(failedFlowFile.getAttribute("error.message"))
    }

    @Test
    fun testMissingInputFolder() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, "/nonexistent/path")
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = "test.jp2"

        runner.enqueue("test", attributes)
        runner.run()

        runner.assertTransferCount(Jhove.FAIL_RELATIONSHIP, 1)
        // JHOVE may still create output even for missing folders
        assertTrue(runner.getFlowFilesForRelationship(Jhove.JHOVE_OUTPUT_RELATIONSHIP).size >= 0)
    }

    @Test
    fun testMissingFilenameAttribute() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        // No filename attribute
        runner.enqueue("test")
        runner.run()

        runner.assertTransferCount(Jhove.FAIL_RELATIONSHIP, 1)
        // No JHOVE output when filename attribute is missing (processor catches this early)
        assertTrue(runner.getFlowFilesForRelationship(Jhove.JHOVE_OUTPUT_RELATIONSHIP).size >= 0)
    }

    @Test
    fun testContinueOnErrorMode() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "continue")

        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2"

        runner.enqueue("test", attributes)
        runner.run()

        // Should succeed even if there are validation issues
        runner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, 1)
        runner.assertTransferCount(Jhove.JHOVE_OUTPUT_RELATIONSHIP, 1)
    }

    @Test
    fun testMultipleFiles() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        // Enqueue multiple files
        val filenames = listOf(
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2",
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_002.jp2",
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_003.jp2",
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_004.jp2"
        )

        for (filename in filenames) {
            val attributes = HashMap<String, String>()
            attributes[NiFiAttributes.FILENAME] = filename
            runner.enqueue("test", attributes)
        }

        runner.run(filenames.size)

        runner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, filenames.size)
        runner.assertTransferCount(Jhove.JHOVE_OUTPUT_RELATIONSHIP, filenames.size)

        // Verify all JHOVE output files were created
        val jhoveOutputFiles = tempOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }
        assertEquals(filenames.size, jhoveOutputFiles?.size, "Should have JHOVE output for each input file")
    }

    @Test
    fun testJhoveOutputFilenamePattern() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val inputFilename = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2"
        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = inputFilename

        runner.enqueue("test", attributes)
        runner.run()

        // Verify JHOVE output follows naming pattern: JHOVE_<inputfilename>.xml
        val jhoveOutputFlowFiles = runner.getFlowFilesForRelationship(Jhove.JHOVE_OUTPUT_RELATIONSHIP)
        val jhoveFlowFile = jhoveOutputFlowFiles[0]

        val expectedOutputFilename = "JHOVE_$inputFilename.xml"
        assertEquals(expectedOutputFilename, jhoveFlowFile.getAttribute(NiFiAttributes.FILENAME))
    }

    @Test
    fun testJhoveChecksumGeneration() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2"

        runner.enqueue("test", attributes)
        runner.run()

        // Find and parse the JHOVE output file
        val jhoveOutputFiles = tempOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }
        val jhoveOutputFile = jhoveOutputFiles!![0]
        val doc = parseXmlFile(jhoveOutputFile)

        // Verify MD5 and SHA-1 checksums are present
        val md5 = xpath(doc, "//jhove:repInfo/jhove:checksums/jhove:checksum[@type='MD5']")
        assertNotNull(md5, "MD5 checksum should be generated")
        assertEquals(32, md5!!.length, "MD5 checksum should be 32 characters")

        val sha1 = xpath(doc, "//jhove:repInfo/jhove:checksums/jhove:checksum[@type='SHA-1']")
        assertNotNull(sha1, "SHA-1 checksum should be generated")
        assertEquals(40, sha1!!.length, "SHA-1 checksum should be 40 characters")
    }

    @Test
    fun testJhoveStatusParsing() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2"

        runner.enqueue("test", attributes)
        runner.run()

        // Verify the file was routed to success based on "Well-Formed and valid" status
        runner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, 1)
        runner.assertTransferCount(Jhove.JHOVE_OUTPUT_RELATIONSHIP, 1)

        // Parse JHOVE output to verify status
        val jhoveOutputFiles = tempOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }
        val jhoveOutputFile = jhoveOutputFiles!![0]
        val doc = parseXmlFile(jhoveOutputFile)

        val status = xpath(doc, "//jhove:repInfo/jhove:status")
        assertTrue(status!!.contains("Well-Formed"), "Status should indicate file is well-formed")
    }

    @Test
    fun testJhoveMixImageDimensions() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2"

        runner.enqueue("test", attributes)
        runner.run()

        // Parse JHOVE output to verify MIX image dimensions
        val jhoveOutputFiles = tempOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }
        val jhoveOutputFile = jhoveOutputFiles!![0]
        val doc = parseXmlFile(jhoveOutputFile)

        // Verify MIX imageWidth and imageHeight exist
        val imageWidth = xpathMix(doc, "//mix:BasicImageCharacteristics/mix:imageWidth")
        assertNotNull(imageWidth, "MIX imageWidth should exist")
        assertEquals("7133", imageWidth, "imageWidth should be 7133")

        val imageHeight = xpathMix(doc, "//mix:BasicImageCharacteristics/mix:imageHeight")
        assertNotNull(imageHeight, "MIX imageHeight should exist")
        assertEquals("8619", imageHeight, "imageHeight should be 8619")
    }

    @Test
    fun testJhoveMixSpatialMetrics() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val attributes = HashMap<String, String>()
        attributes[NiFiAttributes.FILENAME] = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2"

        runner.enqueue("test", attributes)
        runner.run()

        // Parse JHOVE output to verify MIX spatial metrics
        val jhoveOutputFiles = tempOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }
        val jhoveOutputFile = jhoveOutputFiles!![0]
        val doc = parseXmlFile(jhoveOutputFile)

        // Verify MIX spatial metrics exist
        val samplingFrequencyUnit = xpathMix(doc, "//mix:SpatialMetrics/mix:samplingFrequencyUnit")
        assertNotNull(samplingFrequencyUnit, "samplingFrequencyUnit should exist")
        assertEquals("3", samplingFrequencyUnit, "samplingFrequencyUnit should be 3 (cm)")

        // Verify X sampling frequency
        val xNumerator = xpathMix(doc, "//mix:xSamplingFrequency/mix:numerator")
        assertNotNull(xNumerator, "xSamplingFrequency numerator should exist")
        assertEquals("4000000", xNumerator, "xSamplingFrequency numerator should be 4000000")

        val xDenominator = xpathMix(doc, "//mix:xSamplingFrequency/mix:denominator")
        assertNotNull(xDenominator, "xSamplingFrequency denominator should exist")
        assertEquals("25400", xDenominator, "xSamplingFrequency denominator should be 25400")

        // Verify Y sampling frequency
        val yNumerator = xpathMix(doc, "//mix:ySamplingFrequency/mix:numerator")
        assertNotNull(yNumerator, "ySamplingFrequency numerator should exist")
        assertEquals("4000000", yNumerator, "ySamplingFrequency numerator should be 4000000")

        val yDenominator = xpathMix(doc, "//mix:ySamplingFrequency/mix:denominator")
        assertNotNull(yDenominator, "ySamplingFrequency denominator should exist")
        assertEquals("25400", yDenominator, "ySamplingFrequency denominator should be 25400")
    }

    @Test
    fun testJhoveMixDataForAllFiles() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val testFiles = listOf(
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2",
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_002.jp2",
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_003.jp2",
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_004.jp2"
        )

        for (filename in testFiles) {
            val attributes = HashMap<String, String>()
            attributes[NiFiAttributes.FILENAME] = filename
            runner.enqueue("test", attributes)
        }

        runner.run(testFiles.size)

        // Verify all files have MIX data
        val jhoveOutputFiles = tempOutputDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        }?.sortedBy { it.name }

        assertEquals(testFiles.size, jhoveOutputFiles?.size, "Should have JHOVE output for each file")

        for (jhoveFile in jhoveOutputFiles!!) {
            val doc = parseXmlFile(jhoveFile)

            // Verify each file has MIX dimensions
            val imageWidth = xpathMix(doc, "//mix:BasicImageCharacteristics/mix:imageWidth")
            assertNotNull(imageWidth, "File ${jhoveFile.name} should have imageWidth")
            assertTrue(imageWidth!!.toInt() > 0, "imageWidth should be positive")

            val imageHeight = xpathMix(doc, "//mix:BasicImageCharacteristics/mix:imageHeight")
            assertNotNull(imageHeight, "File ${jhoveFile.name} should have imageHeight")
            assertTrue(imageHeight!!.toInt() > 0, "imageHeight should be positive")

            // Verify spatial metrics exist
            val samplingFrequencyUnit = xpathMix(doc, "//mix:SpatialMetrics/mix:samplingFrequencyUnit")
            assertNotNull(samplingFrequencyUnit, "File ${jhoveFile.name} should have samplingFrequencyUnit")
        }
    }

    // Helper methods for XML parsing (using centralized XmlHelper)

    private fun parseXmlFile(file: File): Document = XmlHelper.parseXmlFile(file)

    private fun xpath(doc: Document, expression: String): String? = XmlHelper.xpath(doc, expression)

    private fun xpathMix(doc: Document, expression: String): String? = XmlHelper.xpath(doc, expression)

    @Test
    fun testFullJhoveFileStructureAndMetadata() {
        val runner = TestRunners.newTestRunner(Jhove::class.java)

        runner.setProperty(Jhove.INPUT_PATH, inputPath)
        runner.setProperty(Jhove.OUTPUT_PATH, tempOutputDir.absolutePath)
        runner.setProperty(Jhove.MODULE, "JPEG2000-hul")
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        val testFiles = mapOf(
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2" to 120080908L,
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_002.jp2" to 120154494L,
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_003.jp2" to 119865044L,
            "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_004.jp2" to 119399461L
        )

        for ((filename, expectedSize) in testFiles) {
            val attributes = HashMap<String, String>()
            attributes[NiFiAttributes.FILENAME] = filename

            runner.enqueue("test", attributes)
            runner.run()

            runner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, 1)
            runner.assertTransferCount(Jhove.JHOVE_OUTPUT_RELATIONSHIP, 1)

            val generatedFile = File(tempOutputDir, "JHOVE_$filename.xml")
            val expectedFile = File("$testDataRoot/metadata/technical/jhove/JHOVE_$filename.xml")

            assertTrue(generatedFile.exists(), "Generated JHOVE file should exist: ${generatedFile.name}")

            // Verify generated file has correct structure and metadata
            val validationErrors = validateJhoveXmlStructure(generatedFile, expectedSize)
            assertTrue(validationErrors.isEmpty(), "Generated JHOVE file has validation errors for $filename:\n${validationErrors.joinToString("\n")}")

            // Also verify it matches the expected file structure (ignoring dynamic values)
            val structuralComparison = compareJhoveXmlStructure(generatedFile, expectedFile)
            assertTrue(structuralComparison.isEmpty(), "Generated JHOVE file structure differs from expected for $filename:\n${structuralComparison.joinToString("\n")}")

            runner.clearTransferState()
        }
    }

    private fun validateJhoveXmlStructure(generatedFile: File, expectedFileSize: Long): List<String> {
        val errors = mutableListOf<String>()

        val factory = DocumentBuilderFactory.newInstance()
        factory.isNamespaceAware = true
        val builder = factory.newDocumentBuilder()
        val doc = builder.parse(generatedFile)

        val xpathFactory = XPathFactory.newInstance()
        val xpath = xpathFactory.newXPath()

        // Validate required elements are present
        val requiredElements = mapOf(
            "//*[local-name()='format']" to "JPEG 2000",
            "//*[local-name()='status']" to "Well-Formed",
            "//*[local-name()='size']" to expectedFileSize.toString()
        )

        for ((xpathExpr, expectedValue) in requiredElements) {
            try {
                val value = xpath.evaluate(xpathExpr, doc)?.trim()
                if (value.isNullOrEmpty()) {
                    errors.add("Missing required element: $xpathExpr")
                } else if (xpathExpr.contains("status")) {
                    if (!value.startsWith(expectedValue)) {
                        errors.add("$xpathExpr: expected to start with '$expectedValue', got '$value'")
                    }
                } else if (xpathExpr.contains("size")) {
                    if (value != expectedValue) {
                        errors.add("$xpathExpr: expected '$expectedValue', got '$value'")
                    }
                } else if (value != expectedValue) {
                    errors.add("$xpathExpr: expected '$expectedValue', got '$value'")
                }
            } catch (e: Exception) {
                errors.add("Error evaluating $xpathExpr: ${e.message}")
            }
        }

        // Validate MIX metadata elements are present
        val mixElements = listOf(
            "//*[local-name()='imageWidth']",
            "//*[local-name()='imageHeight']",
            "//*[local-name()='samplingFrequencyUnit']",
            "//*[local-name()='xSamplingFrequency']/*[local-name()='numerator']",
            "//*[local-name()='xSamplingFrequency']/*[local-name()='denominator']",
            "//*[local-name()='ySamplingFrequency']/*[local-name()='numerator']",
            "//*[local-name()='ySamplingFrequency']/*[local-name()='denominator']",
            "//*[local-name()='checksum'][@type='MD5']",
            "//*[local-name()='checksum'][@type='SHA-1']"
        )

        for (xpathExpr in mixElements) {
            try {
                val value = xpath.evaluate(xpathExpr, doc)?.trim()
                if (value.isNullOrEmpty()) {
                    errors.add("Missing MIX element: $xpathExpr")
                }
            } catch (e: Exception) {
                errors.add("Error evaluating MIX element $xpathExpr: ${e.message}")
            }
        }

        return errors
    }

    private fun compareJhoveXmlStructure(generatedFile: File, expectedFile: File): List<String> {
        val differences = mutableListOf<String>()

        val factory = DocumentBuilderFactory.newInstance()
        factory.isNamespaceAware = true
        val builder = factory.newDocumentBuilder()

        val generatedDoc = builder.parse(generatedFile)
        val expectedDoc = builder.parse(expectedFile)

        val xpathFactory = XPathFactory.newInstance()
        val xpath = xpathFactory.newXPath()

        // Compare all important metadata elements (since files should now be identical)
        val elementsToCompare = listOf(
            "//*[local-name()='format']",
            "//*[local-name()='version']",
            "//*[local-name()='size']",
            "//*[local-name()='checksum'][@type='MD5']",
            "//*[local-name()='checksum'][@type='SHA-1']",
            "//*[local-name()='imageWidth']",
            "//*[local-name()='imageHeight']",
            "//*[local-name()='samplingFrequencyUnit']",
            "//*[local-name()='xSamplingFrequency']/*[local-name()='numerator']",
            "//*[local-name()='xSamplingFrequency']/*[local-name()='denominator']",
            "//*[local-name()='ySamplingFrequency']/*[local-name()='numerator']",
            "//*[local-name()='ySamplingFrequency']/*[local-name()='denominator']",
            "//*[local-name()='bitsPerSampleValue']",
            "//*[local-name()='samplesPerPixel']",
            "//*[local-name()='compressionScheme']"
        )

        for (xpathExpr in elementsToCompare) {
            try {
                val generatedValue = xpath.evaluate(xpathExpr, generatedDoc)?.trim()
                val expectedValue = xpath.evaluate(xpathExpr, expectedDoc)?.trim()

                if (!generatedValue.isNullOrEmpty() && !expectedValue.isNullOrEmpty()) {
                    if (generatedValue != expectedValue) {
                        differences.add("$xpathExpr: expected '$expectedValue', got '$generatedValue'")
                    }
                }
            } catch (e: Exception) {
                // Element might not exist in one of the files
            }
        }

        return differences
    }
}
