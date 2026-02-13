package no.nb.nifi.tekst.jhove

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Paths

class JhoveParserTest {

    private val projectFolder = Paths.get("").toAbsolutePath().toString()
    private val testDataRoot = "$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72/primary"
    private val jhoveFolder = "$testDataRoot/metadata/technical/jhove"

    @Test
    fun testParseImageDimensions() {
        val jhoveFile = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser = JhoveParser(jhoveFile)

        // Verify image dimensions
        assertEquals(7133.toBigInteger(), parser.imageWidth, "Image width should be 7133")
        assertEquals(8619.toBigInteger(), parser.imageHeigth, "Image height should be 8619")
    }

    @Test
    fun testParseSamplingFrequencyUnit() {
        val jhoveFile = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser = JhoveParser(jhoveFile)

        // For JP2 files, sampling frequency unit should be 3 (cm)
        val samplingFrequencyUnit = parser.samplingFrequencyUnit()
        assertNotNull(samplingFrequencyUnit, "Sampling frequency unit should not be null")
        assertEquals(3.toBigInteger(), samplingFrequencyUnit, "Sampling frequency unit should be 3 (cm)")
    }

    @Test
    fun testParseXSamplingFrequency() {
        val jhoveFile = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser = JhoveParser(jhoveFile)

        // Test X sampling frequency components
        val xNumerator = parser.xNumerator()
        assertNotNull(xNumerator, "X numerator should not be null")

        // For JP2, this is calculated from JHOVE data (resolution in dots per cm)
        assertTrue(xNumerator!! > 0.toBigDecimal(), "X numerator should be positive")

        // X denominator for JP2 should be null (not used in calculation)
        val xDenominator = parser.xDenominator()
        // For JP2 files, this returns the resolution data
        assertNotNull(xDenominator)
    }

    @Test
    fun testParseYSamplingFrequency() {
        val jhoveFile = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser = JhoveParser(jhoveFile)

        // Test Y sampling frequency components
        val yNumerator = parser.yNumerator()
        assertNotNull(yNumerator, "Y numerator should not be null")
        assertTrue(yNumerator!! > 0.toBigDecimal(), "Y numerator should be positive")

        // Y denominator for JP2 should be null (not used in calculation)
        val yDenominator = parser.yDenominator()
        assertNotNull(yDenominator)
    }

    @Test
    fun testParseFileMetadata() {
        val jhoveFile = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser = JhoveParser(jhoveFile)

        // Test basic file metadata
        assertNotNull(parser.size, "File size should not be null")
        assertTrue(parser.size!! > 0, "File size should be positive")

        assertNotNull(parser.formatName, "Format name should not be null")
        assertEquals("JPEG 2000", parser.formatName, "Format should be JPEG 2000")

        assertNotNull(parser.dateLastModified, "Last modified date should not be null")
        assertNotNull(parser.date, "Date should not be null")
    }

    @Test
    fun testParseChecksums() {
        val jhoveFile = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser = JhoveParser(jhoveFile)

        // Test checksums
        val md5 = parser.mD5Checksum
        assertNotNull(md5, "MD5 checksum should not be null")
        assertEquals(32, md5!!.length, "MD5 should be 32 characters")

        val sha1 = parser.sHA1Checksum
        assertNotNull(sha1, "SHA-1 checksum should not be null")
        assertEquals(40, sha1!!.length, "SHA-1 should be 40 characters")
    }

    @Test
    fun testParseStatus() {
        val jhoveFile = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser = JhoveParser(jhoveFile)

        // Test status
        val status = parser.status
        assertNotNull(status, "Status should not be null")
        assertTrue(status!!.contains("Well-Formed"), "Status should indicate well-formed")

        // Test statusOK
        assertTrue(parser.statusOK(), "Status should be OK")
    }

    @Test
    fun testParseOriginalName() {
        val jhoveFile = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser = JhoveParser(jhoveFile)

        // Test original filename extraction
        val originalName = parser.originalName
        assertNotNull(originalName, "Original name should not be null")
        assertTrue(originalName!!.endsWith(".jp2"), "Original name should end with .jp2")
        assertTrue(originalName.contains("tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72"), "Original name should contain 'tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72'")
    }

    @Test
    fun testFileSuffix() {
        val jhoveFile = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser = JhoveParser(jhoveFile)

        // Test file suffix detection
        assertEquals("jp2", parser.fileSuffix, "File suffix should be jp2")
    }

    @Test
    fun testMultipleFiles() {
        val testFiles = listOf(
            "JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml",
            "JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_002.jp2.xml",
            "JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_003.jp2.xml",
            "JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_004.jp2.xml"
        )

        for (filename in testFiles) {
            val jhoveFile = File("$jhoveFolder/$filename")
            assertTrue(jhoveFile.exists(), "Test file should exist: $filename")

            val parser = JhoveParser(jhoveFile)

            // Verify all files have valid dimensions
            assertNotNull(parser.imageWidth, "File $filename should have imageWidth")
            assertTrue(parser.imageWidth > 0.toBigInteger(), "imageWidth should be positive for $filename")

            assertNotNull(parser.imageHeigth, "File $filename should have imageHeight")
            assertTrue(parser.imageHeigth > 0.toBigInteger(), "imageHeight should be positive for $filename")

            // Verify all files have valid status
            assertTrue(parser.statusOK(), "File $filename should have OK status")

            // Verify all files have checksums
            assertNotNull(parser.mD5Checksum, "File $filename should have MD5")
            assertNotNull(parser.sHA1Checksum, "File $filename should have SHA-1")
        }
    }

    @Test
    fun testImageDimensionsConsistency() {
        // All test files should have their specific dimensions
        val file1 = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser1 = JhoveParser(file1)

        // Verify dimensions are consistent with what we expect
        val width1 = parser1.imageWidth
        val height1 = parser1.imageHeigth

        assertTrue(width1 > 1000.toBigInteger(), "Image width should be reasonable (>1000px)")
        assertTrue(height1 > 1000.toBigInteger(), "Image height should be reasonable (>1000px)")

        // Verify aspect ratio is reasonable for newspaper pages (should be taller than wide)
        assertTrue(height1 > width1, "Newspaper pages should typically be taller than wide")
    }

    @Test
    fun testSamplingFrequencyCalculation() {
        val jhoveFile = File("$jhoveFolder/JHOVE_tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72_001.jp2.xml")
        val parser = JhoveParser(jhoveFile)

        // For JP2 files, verify the resolution calculation
        val xNumerator = parser.xNumerator()
        val yNumerator = parser.yNumerator()

        // Both should be equal for square pixels
        assertEquals(xNumerator, yNumerator, "X and Y resolution should be equal for square pixels")

        // Resolution should be in a reasonable range (typically 150-600 DPI for scanned newspapers)
        // The numerator represents dots per cm after conversion
        assertNotNull(xNumerator)
        assertTrue(xNumerator!! > 50.toBigDecimal(), "Resolution should be at least 50 DPC")
        assertTrue(xNumerator < 1000.toBigDecimal(), "Resolution should be less than 1000 DPC")
    }
}
