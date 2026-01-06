package no.nb.nifi.tekst.validation

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.io.File

/**
 * Tests for XSD validation of JHOVE XML output.
 */
class JhoveXsdValidationTest {

    private val testResourcesPath = "src/test/resources"
    private val tekstUuid = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72"
    private val jhoveFolder = "$testResourcesPath/$tekstUuid/primary/metadata/technical/jhove"

    @Test
    fun `validate JHOVE output file 001 against XSD`() {
        val jhoveFile = File("$jhoveFolder/JHOVE_${tekstUuid}_001.jp2.xml")
        assertTrue(jhoveFile.exists(), "JHOVE file should exist: ${jhoveFile.absolutePath}")

        val result = XsdValidator.validateJhove(jhoveFile)

        if (!result.isValid) {
            println("Validation errors for ${jhoveFile.name}:")
            result.errors.forEach { println("  - $it") }
        }

        result.assertValid()
    }

    @Test
    fun `validate JHOVE output file 002 against XSD`() {
        val jhoveFile = File("$jhoveFolder/JHOVE_${tekstUuid}_002.jp2.xml")
        assertTrue(jhoveFile.exists(), "JHOVE file should exist: ${jhoveFile.absolutePath}")

        val result = XsdValidator.validateJhove(jhoveFile)

        if (!result.isValid) {
            println("Validation errors for ${jhoveFile.name}:")
            result.errors.forEach { println("  - $it") }
        }

        result.assertValid()
    }

    @Test
    fun `validate JHOVE output file 003 against XSD`() {
        val jhoveFile = File("$jhoveFolder/JHOVE_${tekstUuid}_003.jp2.xml")
        assertTrue(jhoveFile.exists(), "JHOVE file should exist: ${jhoveFile.absolutePath}")

        val result = XsdValidator.validateJhove(jhoveFile)

        if (!result.isValid) {
            println("Validation errors for ${jhoveFile.name}:")
            result.errors.forEach { println("  - $it") }
        }

        result.assertValid()
    }

    @Test
    fun `validate JHOVE output file 004 against XSD`() {
        val jhoveFile = File("$jhoveFolder/JHOVE_${tekstUuid}_004.jp2.xml")
        assertTrue(jhoveFile.exists(), "JHOVE file should exist: ${jhoveFile.absolutePath}")

        val result = XsdValidator.validateJhove(jhoveFile)

        if (!result.isValid) {
            println("Validation errors for ${jhoveFile.name}:")
            result.errors.forEach { println("  - $it") }
        }

        result.assertValid()
    }

    @Test
    fun `validate all JHOVE files in test resources against XSD`() {
        val jhoveDir = File(jhoveFolder)
        assertTrue(jhoveDir.exists() && jhoveDir.isDirectory, "JHOVE folder should exist")

        val jhoveFiles = jhoveDir.listFiles { file ->
            file.name.startsWith("JHOVE_") && file.name.endsWith(".xml")
        } ?: emptyArray()

        assertTrue(jhoveFiles.isNotEmpty(), "Should have at least one JHOVE file")

        val failures = mutableListOf<String>()

        jhoveFiles.forEach { file ->
            val result = XsdValidator.validateJhove(file)
            if (!result.isValid) {
                failures.add("${file.name}: ${result.errors.joinToString("; ")}")
            }
        }

        if (failures.isNotEmpty()) {
            fail<Unit>("JHOVE XSD validation failures:\n${failures.joinToString("\n")}")
        }

        println("Successfully validated ${jhoveFiles.size} JHOVE files against XSD")
    }
}

