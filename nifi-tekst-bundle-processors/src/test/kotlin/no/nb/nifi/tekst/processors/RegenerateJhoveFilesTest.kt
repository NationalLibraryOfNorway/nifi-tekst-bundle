package no.nb.nifi.tekst.processors

import no.nb.nifi.tekst.util.NiFiAttributes
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files

/**
 * Utility test to regenerate JHOVE XML files for test resources.
 *
 * This test is @Disabled by default - run it manually when you need to
 * regenerate the JHOVE output files with the current JHOVE library version.
 */
class RegenerateJhoveFilesTest {

    private val projectFolder = File("").absolutePath
    private val testResourcesBase = "$projectFolder/src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72"

    @Test
    @Disabled("Run manually to regenerate JHOVE files")
    fun regenerateAllJhoveFiles() {
        // Regenerate primary JP2 JHOVE files
        regenerateJhoveForFolder(
            inputFolder = "$testResourcesBase/primary/data",
            outputFolder = "$testResourcesBase/primary/metadata/technical/jhove",
            filePattern = "*.jp2",
            module = "JPEG2000-hul"
        )

        // Regenerate access JP2 JHOVE files
        regenerateJhoveForFolder(
            inputFolder = "$testResourcesBase/access/data",
            outputFolder = "$testResourcesBase/access/metadata/technical/jhove",
            filePattern = "*.jp2",
            module = "JPEG2000-hul"
        )

        // Regenerate ALTO XML JHOVE files (OCR files)
        regenerateJhoveForFolder(
            inputFolder = "$testResourcesBase/access/metadata/other/ocr",
            outputFolder = "$testResourcesBase/access/metadata/other/jhove-ocr",
            filePattern = "*.xml",
            module = "XML-hul"
        )

        println("JHOVE files regenerated successfully!")
    }

    private fun regenerateJhoveForFolder(
        inputFolder: String,
        outputFolder: String,
        filePattern: String,
        module: String
    ) {
        val inputDir = File(inputFolder)
        val outputDir = File(outputFolder)

        if (!inputDir.exists()) {
            println("Input folder does not exist: $inputFolder")
            return
        }

        // Ensure output folder exists
        outputDir.mkdirs()

        val files = inputDir.listFiles { file ->
            file.name.endsWith(filePattern.removePrefix("*"))
        } ?: return

        println("Processing ${files.size} files from $inputFolder")

        for (file in files) {
            val runner = TestRunners.newTestRunner(Jhove::class.java)

            runner.setProperty(Jhove.INPUT_PATH, inputFolder)
            runner.setProperty(Jhove.OUTPUT_PATH, outputFolder)
            runner.setProperty(Jhove.MODULE, module)
            runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

            val attributes = HashMap<String, String>()
            attributes[NiFiAttributes.FILENAME] = file.name

            runner.enqueue("test", attributes)
            runner.run()

            val successCount = runner.getFlowFilesForRelationship(Jhove.SUCCESS_RELATIONSHIP).size
            val jhoveCount = runner.getFlowFilesForRelationship(Jhove.JHOVE_OUTPUT_RELATIONSHIP).size

            if (successCount > 0 && jhoveCount > 0) {
                println("  ✓ ${file.name} -> JHOVE_${file.name}.xml")
            } else {
                val failCount = runner.getFlowFilesForRelationship(Jhove.FAIL_RELATIONSHIP).size
                println("  ✗ ${file.name} - FAILED (success=$successCount, jhove=$jhoveCount, fail=$failCount)")
            }

            runner.clearTransferState()
        }
    }
}

