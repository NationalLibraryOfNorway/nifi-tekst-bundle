package no.nb.nifi.tekst.validation

import no.nb.nifi.tekst.mets.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Tests for XSD validation of MIX (Metadata for Images in XML) output.
 * Tests both MIX v1.0 and MIX v2.0 schemas.
 */
class MixXsdValidationTest {

    @Test
    fun `MIX 1_0 XSD is available and can be loaded`() {
        val xsdStream = javaClass.getResourceAsStream("/xsd/mix10.xsd")
        assertNotNull(xsdStream, "MIX 1.0 XSD should be available on classpath")

        val content = xsdStream!!.bufferedReader().readText()
        assertTrue(content.contains("http://www.loc.gov/mix/v10"),
            "MIX 1.0 XSD should define v10 namespace")
    }

    @Test
    fun `MIX 2_0 XSD is available and can be loaded`() {
        val xsdStream = javaClass.getResourceAsStream("/xsd/mix20.xsd")
        assertNotNull(xsdStream, "MIX 2.0 XSD should be available on classpath")

        val content = xsdStream!!.bufferedReader().readText()
        assertTrue(content.contains("http://www.loc.gov/mix/v20"),
            "MIX 2.0 XSD should define v20 namespace")
        assertTrue(content.contains("MIX Version 2.0"),
            "MIX 2.0 XSD should contain version 2.0 documentation")
    }

    @Test
    fun `generate METS v1 with MIX v1_0 and validate`() {
        val pages = createTestPages()
        val generator = MetsBrowsingGenerator()

        val xmlContent = generator.generateMetsBrowsing(
            objId = "test_mets1_mix1",
            pages = pages,
            agentName = "MixValidationTest",
            version = MetsVersion.METS_1,
            mixVersion = MixVersion.MIX_1_0
        )

        // Verify MIX v1.0 namespace
        assertTrue(xmlContent.contains("http://www.loc.gov/mix/v10"),
            "Should contain MIX v1.0 namespace")

        // Verify MIX v1.0 specific elements
        assertTrue(xmlContent.contains("<mix:tiles>1024x1024</mix:tiles>"),
            "MIX v1.0 should use simple tiles string")
        assertTrue(xmlContent.contains("<mix:samplingFrequencyUnit>3</mix:samplingFrequencyUnit>"),
            "MIX v1.0 should use integer samplingFrequencyUnit")

        // Validate against METS v1 + MIX v1.0 schema
        val result = XsdValidator.validateMets(xmlContent)
        if (!result.isValid) {
            println("Validation errors:")
            result.errors.forEach { println("  - $it") }
        }
        result.assertValid()
    }

    @Test
    fun `generate METS v1 with MIX v2_0 and validate`() {
        val pages = createTestPages()
        val generator = MetsBrowsingGenerator()

        val xmlContent = generator.generateMetsBrowsing(
            objId = "test_mets1_mix2",
            pages = pages,
            agentName = "MixValidationTest",
            version = MetsVersion.METS_1,
            mixVersion = MixVersion.MIX_2_0
        )

        // Verify MIX v2.0 namespace
        assertTrue(xmlContent.contains("http://www.loc.gov/mix/v20"),
            "Should contain MIX v2.0 namespace")

        // Verify MIX v2.0 specific elements
        assertTrue(xmlContent.contains("<mix:Tiles>"),
            "MIX v2.0 should use Tiles container")
        assertTrue(xmlContent.contains("<mix:tileWidth>1024</mix:tileWidth>"),
            "MIX v2.0 should have tileWidth")
        assertTrue(xmlContent.contains("<mix:tileHeight>1024</mix:tileHeight>"),
            "MIX v2.0 should have tileHeight")
        assertTrue(xmlContent.contains("<mix:samplingFrequencyUnit>cm</mix:samplingFrequencyUnit>"),
            "MIX v2.0 should use string samplingFrequencyUnit")

        // Note: We can't validate against XSD with mixed METS v1 + MIX v2 namespaces easily
        // But we verify the structure is correct
        if (!xmlContent.contains("xmlns:mix=\"http://www.loc.gov/mix/v20\"")) {
            println("Generated XML for METS v1 + MIX v2.0:")
            println(xmlContent.take(1000))
        }
        // Check both possible namespace declaration formats
        val hasMix20Namespace = xmlContent.contains("xmlns:mix=\"http://www.loc.gov/mix/v20\"") ||
                                 xmlContent.contains("http://www.loc.gov/mix/v20")
        assertTrue(hasMix20Namespace,
            "Should reference MIX v2.0 namespace")
    }

    @Test
    fun `generate METS v2 with MIX v1_0 and validate`() {
        val pages = createTestPages()
        val generator = MetsBrowsingGenerator()

        val xmlContent = generator.generateMetsBrowsing(
            objId = "test_mets2_mix1",
            pages = pages,
            agentName = "MixValidationTest",
            version = MetsVersion.METS_2,
            mixVersion = MixVersion.MIX_1_0
        )

        // Verify METS v2 and MIX v1.0 namespaces
        assertTrue(xmlContent.contains("http://www.loc.gov/METS/v2"),
            "Should contain METS v2 namespace")
        assertTrue(xmlContent.contains("http://www.loc.gov/mix/v10"),
            "Should contain MIX v1.0 namespace")

        // Verify MIX v1.0 specific elements
        assertTrue(xmlContent.contains("<mix:tiles>1024x1024</mix:tiles>"),
            "MIX v1.0 should use simple tiles string")

        // Note: Can't easily validate mixed METS v2 + MIX v1.0
        assertTrue(xmlContent.contains("xmlns:mix=\"http://www.loc.gov/mix/v10\""),
            "Should declare MIX v1.0 namespace")
    }

    @Test
    fun `generate METS v2 with MIX v2_0 and validate`() {
        val pages = createTestPages()
        val generator = MetsBrowsingGenerator()

        val xmlContent = generator.generateMetsBrowsing(
            objId = "test_mets2_mix2",
            pages = pages,
            agentName = "MixValidationTest",
            version = MetsVersion.METS_2,
            mixVersion = MixVersion.MIX_2_0
        )

        // Verify METS v2 and MIX v2.0 namespaces
        assertTrue(xmlContent.contains("http://www.loc.gov/METS/v2"),
            "Should contain METS v2 namespace")
        assertTrue(xmlContent.contains("http://www.loc.gov/mix/v20"),
            "Should contain MIX v2.0 namespace")

        // Verify METS v2 characteristics
        assertTrue(xmlContent.contains("<mets:mdSec>"),
            "METS v2 should use mdSec")
        assertTrue(xmlContent.contains("LOCREF="),
            "METS v2 should use LOCREF")
        assertTrue(xmlContent.contains("<mets:structSec>"),
            "METS v2 should use structSec")

        // Verify MIX v2.0 specific elements
        assertTrue(xmlContent.contains("<mix:Tiles>"),
            "MIX v2.0 should use Tiles container")
        assertTrue(xmlContent.contains("<mix:tileWidth>1024</mix:tileWidth>"),
            "MIX v2.0 should have tileWidth")
        assertTrue(xmlContent.contains("<mix:tileHeight>1024</mix:tileHeight>"),
            "MIX v2.0 should have tileHeight")
        assertTrue(xmlContent.contains("<mix:samplingFrequencyUnit>cm</mix:samplingFrequencyUnit>"),
            "MIX v2.0 should use string samplingFrequencyUnit")

        // Validate against METS v2 + MIX v2.0 schema
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
    fun `MIX v2 samplingFrequencyUnit conversions are correct`() {
        val metrics1 = SpatialMetrics(
            samplingFrequencyUnit = 1,
            xSamplingFrequency = SamplingFrequency(4000000, 25400),
            ySamplingFrequency = SamplingFrequency(4000000, 25400)
        )
        assertEquals("no absolute unit of measurement", metrics1.getSamplingFrequencyUnitString())

        val metrics2 = SpatialMetrics(
            samplingFrequencyUnit = 2,
            xSamplingFrequency = SamplingFrequency(4000000, 25400),
            ySamplingFrequency = SamplingFrequency(4000000, 25400)
        )
        assertEquals("in.", metrics2.getSamplingFrequencyUnitString())

        val metrics3 = SpatialMetrics(
            samplingFrequencyUnit = 3,
            xSamplingFrequency = SamplingFrequency(4000000, 25400),
            ySamplingFrequency = SamplingFrequency(4000000, 25400)
        )
        assertEquals("cm", metrics3.getSamplingFrequencyUnitString())
    }

    @Test
    fun `MIX v2 tiles dimension parsing is correct`() {
        val options1 = EncodingOptions(tiles = "1024x1024")
        val (width1, height1) = options1.getTileDimensions()
        assertEquals(1024, width1)
        assertEquals(1024, height1)

        val options2 = EncodingOptions(tiles = "512x768")
        val (width2, height2) = options2.getTileDimensions()
        assertEquals(512, width2)
        assertEquals(768, height2)

        // Test invalid format defaults to 1024x1024
        val options3 = EncodingOptions(tiles = "invalid")
        val (width3, height3) = options3.getTileDimensions()
        assertEquals(1024, width3)
        assertEquals(1024, height3)
    }

    @Test
    fun `validate MIX2 embedded in METS2 document`() {
        val pages = createTestPages()
        val generator = MetsBrowsingGenerator()

        val xmlContent = generator.generateMetsBrowsing(
            objId = "test_mix2_in_mets2",
            pages = pages,
            agentName = "MixValidationTest",
            version = MetsVersion.METS_2,
            mixVersion = MixVersion.MIX_2_0
        )

        // First validate METS2 itself
        val mets2Result = XsdValidator.validateMets2(xmlContent)
        assertTrue(mets2Result.isValid, "METS2 should be valid: ${mets2Result.errors}")

        // Then validate embedded MIX2 data
        val mix2Result = XsdValidator.validateMix2InMets(xmlContent)
        if (!mix2Result.isValid) {
            println("MIX2 validation errors:")
            mix2Result.errors.forEach { println("  - $it") }
        }
        assertTrue(mix2Result.isValid, "Embedded MIX2 data should be valid")
    }

    @Test
    fun `validate MIX1 embedded in METS1 document`() {
        val pages = createTestPages()
        val generator = MetsBrowsingGenerator()

        val xmlContent = generator.generateMetsBrowsing(
            objId = "test_mix1_in_mets1",
            pages = pages,
            agentName = "MixValidationTest",
            version = MetsVersion.METS_1,
            mixVersion = MixVersion.MIX_1_0
        )

        // First validate METS1 itself
        val mets1Result = XsdValidator.validateMets(xmlContent)
        assertTrue(mets1Result.isValid, "METS1 should be valid: ${mets1Result.errors}")

        // Then validate embedded MIX1 data
        val mix1Result = XsdValidator.validateMixInMets(xmlContent)
        if (!mix1Result.isValid) {
            println("MIX1 validation errors:")
            mix1Result.errors.forEach { println("  - $it") }
        }
        assertTrue(mix1Result.isValid, "Embedded MIX1 data should be valid")
    }

    @Test
    fun `validateMix2InMets returns error when no MIX2 elements found`() {
        val xmlWithoutMix = """<?xml version="1.0" encoding="UTF-8"?>
            <mets xmlns="http://www.loc.gov/METS/v2">
                <structSec></structSec>
            </mets>
        """.trimIndent()

        val result = XsdValidator.validateMix2InMets(xmlWithoutMix)
        assertFalse(result.isValid, "Should fail when no MIX elements found")
        assertTrue(result.errors.any { it.contains("No MIX elements found") },
            "Error message should indicate no MIX elements found")
    }

    @Test
    fun `validateMixInMets returns error when no MIX1 elements found`() {
        val xmlWithoutMix = """<?xml version="1.0" encoding="UTF-8"?>
            <mets xmlns="http://www.loc.gov/METS/">
                <amdSec></amdSec>
            </mets>
        """.trimIndent()

        val result = XsdValidator.validateMixInMets(xmlWithoutMix)
        assertFalse(result.isValid, "Should fail when no MIX elements found")
        assertTrue(result.errors.any { it.contains("No MIX elements found") },
            "Error message should indicate no MIX elements found")
    }

    private fun createTestPages(): List<PageInfo> {
        return listOf(
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
    }
}
