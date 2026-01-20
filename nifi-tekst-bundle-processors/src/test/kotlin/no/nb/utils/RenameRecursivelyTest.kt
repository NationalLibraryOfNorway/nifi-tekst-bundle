package no.nb.utils

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import no.nb.models.RenameInstruction
import no.nb.utils.RenameUtils.renameAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.io.path.createTempDirectory

object RenameAllManualTest {

    private val mapper = ObjectMapper()

    private fun readFile(fileName: String): JsonNode {
        val resource = this::class.java.classLoader
            .getResource("reorder-files/$fileName")
            ?: error("Resource not found: reorder-files/$fileName")

        return mapper.readTree(File(resource.toURI()))
    }

    /**
     * Copies `src/test/resources/reorder-files` into a temp directory
     * so files can be renamed safely.
     */
    private fun copyResourceDirToTemp(): File {
        val tempDir = createTempDirectory("rename-test-").toFile()
        val targetRoot = File(tempDir, "tmp").apply { mkdirs() }

        val reorderFilesUrl = requireNotNull(
            RenameAllManualTest::class.java.classLoader.getResource("reorder-files")
        ) { "reorder-files not found on classpath" }

        val resourceRoot = File(reorderFilesUrl.toURI())

        resourceRoot.walkTopDown().forEach { source ->
            val relative = source.relativeTo(resourceRoot)
            val target = File(targetRoot, relative.path)

            if (source.isDirectory) {
                target.mkdirs()
            } else {
                source.copyTo(target, overwrite = true)
            }
        }
        return targetRoot
    }

    private fun getDataDirs(baseDir: File, itemId: String): Pair<File, File> {
        val access = File(baseDir, "$itemId/representations/access/data")
        val primary = File(baseDir, "$itemId/representations/primary/data")
        return access to primary
    }

    @Test
    fun `renameAll using renameInstructions`() {
        val tempRoot = copyResourceDirToTemp()
        val jsonFile = readFile("renameInstructions.json")

        val renameInstructions =
            jsonFile["renameInstructions"].map { node ->
                RenameInstruction(
                    originalName = node["originalName"].asText(),
                    newName = node["newName"].asText()
                )
            }

        // Pre-check
        renameInstructions.forEach { instruction ->
            val itemId = instruction.originalName.substringBefore('_')
            val (accessDir, primaryDir) = getDataDirs(tempRoot, itemId)

            assertTrue(File(accessDir, instruction.originalName).exists())
            assertTrue(File(primaryDir, instruction.originalName).exists())
        }

        renameAll(tempRoot.toPath(), renameInstructions)

        val expectedByItem = renameInstructions
            .groupBy { it.newName.substringBefore('_') }
            .mapValues { (_, instructions) -> instructions.map { it.newName }.toSet() }

        expectedByItem.forEach { (itemId, expectedNames) ->
            val (accessDir, primaryDir) = getDataDirs(tempRoot, itemId)

            assertEquals(expectedNames, accessDir.listFiles()?.map { it.name }?.toSet().orEmpty())
            assertEquals(expectedNames, primaryDir.listFiles()?.map { it.name }?.toSet().orEmpty())
        }
    }

    @Test
    fun `renameAll does nothing when originalName equals newName`() {
        val tempRoot = copyResourceDirToTemp()
        val jsonFile = readFile("renameInstructions.json")

        // Get similar instructions
        val renameInstructions = jsonFile["renameInstructions"]
            .filter { node -> node["originalName"].asText() == node["newName"].asText() }
            .map { node ->
                RenameInstruction(
                    originalName = node["originalName"].asText(),
                    newName = node["newName"].asText()
                )
            }

        // Capture pre-state
        val preState = tempRoot.walkTopDown()
            .filter { it.isFile }
            .map { it.relativeTo(tempRoot).path }
            .toSet()

        renameAll(tempRoot.toPath(), renameInstructions)

        // Capture post-state
        val postState = tempRoot.walkTopDown()
            .filter { it.isFile }
            .map { it.relativeTo(tempRoot).path }
            .toSet()

        assertEquals(preState, postState, "No files should be moved or renamed")
    }

    @Test
    fun `renameAll cleans up temporary directory`() {
        val tempRoot = copyResourceDirToTemp()
        val jsonFile = readFile("renameInstructions.json")

        val renameInstructions = jsonFile["renameInstructions"].map { node ->
            RenameInstruction(
                originalName = node["originalName"].asText(),
                newName = node["newName"].asText()
            )
        }

        renameAll(tempRoot.toPath(), renameInstructions)

        // Assert no temp_conflicts_ directory remains
        val tempDirs = tempRoot.listFiles { file ->
            file.isDirectory && file.name.startsWith("temp_conflicts_")
        }
        assertTrue(tempDirs.isNullOrEmpty(), "Temporary directory should be cleaned up")
    }

}
