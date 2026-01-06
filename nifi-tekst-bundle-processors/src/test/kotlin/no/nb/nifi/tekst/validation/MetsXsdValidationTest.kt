package no.nb.nifi.tekst.validation

import no.nb.nifi.tekst.mets.AltoFileInfo
import no.nb.nifi.tekst.mets.ImageFileInfo
import no.nb.nifi.tekst.mets.ImageResolution
import no.nb.nifi.tekst.mets.MetsBrowsingGenerator
import no.nb.nifi.tekst.mets.MetsVersion
import no.nb.nifi.tekst.mets.MixVersion
import no.nb.nifi.tekst.mets.PageInfo
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.io.File

/**
 * Tests for XSD validation of METS-browsing XML output.
 */
class MetsXsdValidationTest {

    private val testResourcesPath = "src/test/resources"
    private val tekstUuid = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72"

    @Test
    fun `validate expected METS-browsing file against XSD`() {
        val metsFile = File("$testResourcesPath/$tekstUuid-METS_BROWSING.xml")
        assertTrue(metsFile.exists(), "Expected METS file should exist: ${metsFile.absolutePath}")

        val result = XsdValidator.validateMets(metsFile)

        if (!result.isValid) {
            println("Validation errors:")
            result.errors.forEach { println("  - $it") }
        }

        result.assertValid()
    }

    @Test
    fun `validate generated METS v2-browsing against XSD`() {
        // Create test data - default is now METS v2 + MIX v2
        val pages = listOf(
            PageInfo(
                pageNumber = 1,
                altoFile = AltoFileInfo(
                    size = 12345,
                    checksum = "abc123def456",
                    urn = "URN:NBN:no-nb_test_001",
                    url = "file://ocr/test_001.xml"
                ),
                imageFile = ImageFileInfo(
                    size = 67890,
                    checksum = "def789abc012",
                    urn = "URN:NBN:no-nb_test_001",
                    url = "file://ocr/test_001.jp2"
                ),
                resolution = ImageResolution(width = 2127, height = 3387)
            ),
            PageInfo(
                pageNumber = 2,
                altoFile = AltoFileInfo(
                    size = 12346,
                    checksum = "abc123def457",
                    urn = "URN:NBN:no-nb_test_002",
                    url = "file://ocr/test_002.xml"
                ),
                imageFile = ImageFileInfo(
                    size = 67891,
                    checksum = "def789abc013",
                    urn = "URN:NBN:no-nb_test_002",
                    url = "file://ocr/test_002.jp2"
                ),
                resolution = ImageResolution(width = 2128, height = 3388)
            )
        )

        // Generate METS
        val generator = MetsBrowsingGenerator()
        val xmlContent = generator.generateMetsBrowsing(
            objId = "test_object_001",
            pages = pages,
            agentName = "XsdValidationTest"
        )


        // Validate against METS v2 XSD (since that's the default now)
        val result = XsdValidator.validateMets2(xmlContent)

        if (!result.isValid) {
            println("Generated XML:")
            println(xmlContent)
            println("\nValidation errors:")
            result.errors.forEach { println("  - $it") }
        }

        result.assertValid()
    }

    @Test
    fun `validate METS v2 with all page types against XSD`() {
        // Test with varying page counts - using METS v2 + MIX v2 (defaults)
        val pages = (1..10).map { pageNum ->
            PageInfo(
                pageNumber = pageNum,
                altoFile = AltoFileInfo(
                    size = 10000L + pageNum,
                    checksum = "checksum$pageNum",
                    urn = "URN:NBN:no-nb_test_${pageNum.toString().padStart(3, '0')}",
                    url = "file://ocr/test_${pageNum.toString().padStart(3, '0')}.xml"
                ),
                imageFile = ImageFileInfo(
                    size = 100000L + pageNum,
                    checksum = "imgchecksum$pageNum",
                    urn = "URN:NBN:no-nb_test_${pageNum.toString().padStart(3, '0')}",
                    url = "file://ocr/test_${pageNum.toString().padStart(3, '0')}.jp2"
                ),
                resolution = ImageResolution(width = 2000 + pageNum, height = 3000 + pageNum)
            )
        }

        val generator = MetsBrowsingGenerator()
        val xmlContent = generator.generateMetsBrowsing(
            objId = "multi_page_test",
            pages = pages,
            agentName = "XsdValidationTest"
            // Uses default METS_2 + MIX_2_0
        )

        // Validate against METS v2 XSD
        val result = XsdValidator.validateMets2(xmlContent)

        if (!result.isValid) {
            println("Validation errors for multi-page METS v2:")
            result.errors.forEach { println("  - $it") }
        }

        result.assertValid()
    }

    @Test
    fun `current METS-browsing uses METS v1 namespace not METS v2`() {
        // This test documents that our current METS-browsing output uses METS v1 (http://www.loc.gov/METS/)
        // and NOT METS v2 (http://www.loc.gov/METS/v2).
        // METS 2 validation will fail because of namespace mismatch.

        val metsFile = File("$testResourcesPath/$tekstUuid-METS_BROWSING.xml")
        assertTrue(metsFile.exists(), "Expected METS file should exist")

        // Should pass METS v1 validation
        val mets1Result = XsdValidator.validateMets(metsFile)
        assertTrue(mets1Result.isValid, "Should be valid against METS v1: ${mets1Result.errors}")

        // Should fail METS v2 validation (different namespace)
        val mets2Result = XsdValidator.validateMets2(metsFile)
        assertFalse(mets2Result.isValid, "Should NOT be valid against METS v2 (different namespace)")
    }

    @Test
    fun `METS 2 XSD is available and can be loaded`() {
        // Verify that the METS 2 XSD resource can be loaded
        val xsdStream = javaClass.getResourceAsStream("/xsd/mets2.xsd")
        assertNotNull(xsdStream, "METS 2 XSD should be available on classpath")

        val content = xsdStream!!.bufferedReader().readText()
        assertTrue(content.contains("http://www.loc.gov/METS/v2"), "METS 2 XSD should define v2 namespace")
        assertTrue(content.contains("METS: Metadata Encoding and Transmission Standard, version 2"),
            "METS 2 XSD should contain version 2 documentation")
    }

    @Test
    fun `validate minimal METS 2 document against XSD`() {
        // Create a minimal valid METS 2 document
        val mets2Xml = """<?xml version="1.0" encoding="UTF-8"?>
<mets xmlns="http://www.loc.gov/METS/v2" 
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://www.loc.gov/METS/v2 https://loc.gov/standards/mets/mets2.xsd"
      OBJID="test_object">
</mets>"""

        val result = XsdValidator.validateMets2(mets2Xml)

        if (!result.isValid) {
            println("METS 2 validation errors:")
            result.errors.forEach { println("  - $it") }
        }

        result.assertValid()
    }

    @Test
    fun `validate METS2_BROWSING file against METS 2 XSD`() {
        val mets2File = File("$testResourcesPath/$tekstUuid-METS2_BROWSING.xml")
        assertTrue(mets2File.exists(), "METS2_BROWSING file should exist: ${mets2File.absolutePath}")

        val result = XsdValidator.validateMets2(mets2File)

        if (!result.isValid) {
            println("METS 2 validation errors for METS2_BROWSING.xml:")
            result.errors.forEach { println("  - $it") }
        }


        result.assertValid()
    }

    @Test
    fun `generate METS 2 and validate against XSD`() {
        // Create test data
        val pages = listOf(
            PageInfo(
                pageNumber = 1,
                altoFile = AltoFileInfo(
                    size = 12345,
                    checksum = "abc123def456",
                    urn = "URN:NBN:no-nb_test_001",
                    url = "file://ocr/test_001.xml"
                ),
                imageFile = ImageFileInfo(
                    size = 67890,
                    checksum = "def789abc012",
                    urn = "URN:NBN:no-nb_test_001",
                    url = "file://ocr/test_001.jp2"
                ),
                resolution = ImageResolution(width = 2127, height = 3387)
            ),
            PageInfo(
                pageNumber = 2,
                altoFile = AltoFileInfo(
                    size = 12346,
                    checksum = "abc123def457",
                    urn = "URN:NBN:no-nb_test_002",
                    url = "file://ocr/test_002.xml"
                ),
                imageFile = ImageFileInfo(
                    size = 67891,
                    checksum = "def789abc013",
                    urn = "URN:NBN:no-nb_test_002",
                    url = "file://ocr/test_002.jp2"
                ),
                resolution = ImageResolution(width = 2128, height = 3388)
            )
        )

        // Generate METS 2
        val generator = MetsBrowsingGenerator()
        val xmlContent = generator.generateMetsBrowsing(
            objId = "test_object_mets2",
            pages = pages,
            agentName = "Mets2ValidationTest",
            version = MetsVersion.METS_2
        )

        // Verify it contains METS 2 namespace
        assertTrue(xmlContent.contains("http://www.loc.gov/METS/v2"),
            "Generated XML should contain METS 2 namespace")
        assertTrue(xmlContent.contains("mets2.xsd"),
            "Generated XML should reference METS 2 XSD")

        // Validate against METS 2 XSD
        val result = XsdValidator.validateMets2(xmlContent)

        if (!result.isValid) {
            println("Generated METS 2 XML:")
            println(xmlContent)
            println("\nValidation errors:")
            result.errors.forEach { println("  - $it") }
        }

        result.assertValid()
    }

    @Test
    fun `generator can produce both METS v1 and METS v2`() {
        val pages = listOf(
            PageInfo(
                pageNumber = 1,
                altoFile = AltoFileInfo(
                    size = 1000,
                    checksum = "abc123",
                    urn = "URN:NBN:no-nb_test_001",
                    url = "file://ocr/test_001.xml"
                ),
                imageFile = ImageFileInfo(
                    size = 2000,
                    checksum = "def456",
                    urn = "URN:NBN:no-nb_test_001",
                    url = "file://ocr/test_001.jp2"
                ),
                resolution = ImageResolution(width = 1000, height = 1500)
            )
        )

        val generator = MetsBrowsingGenerator()

        // Generate METS v1 with MIX v1.0
        val metsV1 = generator.generateMetsBrowsing(
            objId = "test_dual",
            pages = pages,
            version = MetsVersion.METS_1,
            mixVersion = MixVersion.MIX_1_0
        )

        // Generate METS v2 with MIX v2.0
        val metsV2 = generator.generateMetsBrowsing(
            objId = "test_dual",
            pages = pages,
            version = MetsVersion.METS_2,
            mixVersion = MixVersion.MIX_2_0
        )

        // Verify METS v1 characteristics
        assertTrue(metsV1.contains("http://www.loc.gov/METS/\""),
            "METS v1 should have v1 namespace")
        assertTrue(metsV1.contains("amdSec"),
            "METS v1 should use amdSec")
        assertTrue(metsV1.contains("xlink:href"),
            "METS v1 should use xlink:href")

        // Verify METS v2 characteristics
        assertTrue(metsV2.contains("http://www.loc.gov/METS/v2"),
            "METS v2 should have v2 namespace")
        assertTrue(metsV2.contains("mdSec"),
            "METS v2 should use mdSec")
        assertTrue(metsV2.contains("LOCREF"),
            "METS v2 should use LOCREF")
        assertTrue(metsV2.contains("structSec"),
            "METS v2 should wrap structMap in structSec")

        // Validate both
        val v1Result = XsdValidator.validateMets(metsV1)
        val v2Result = XsdValidator.validateMets2(metsV2)

        assertTrue(v1Result.isValid, "METS v1 should be valid: ${v1Result.errors}")
        assertTrue(v2Result.isValid, "METS v2 should be valid: ${v2Result.errors}")
    }
}
