package no.nb.utils

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import no.nb. models.RenameInstruction
import no.nb.utils.RenameUtils.renameAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.io.path.createTempDirectory

class RenameUtilsTest {

    private val mapper = ObjectMapper()
    private lateinit var jsonFile: JsonNode
    private lateinit var tempRoot: File

    private val renameInstructions: List<RenameInstruction>
        get() = jsonFile["renameInstructions"].map { node ->
            RenameInstruction(
                originalName = node["originalName"].asText(),
                newName = node["newName"].asText()
            )
        }

    private fun readFile(): JsonNode {
        val resource = this::class.java.classLoader
            .getResource("reorder-files/renameInstructions.json")
            ?: error("Resource not found: reorder-files/renameInstructions.json")
        return mapper.readTree(File(resource.toURI()))
    }

    @BeforeEach
    fun setUp() {
        jsonFile = readFile()
        tempRoot = copyResourceDirToTemp()
    }

    @AfterEach
    fun tearDown() {
        tempRoot.deleteRecursively()
    }

    /**
     * Copies `src/test/resources/reorder-files` into a temp directory
     * so files can be renamed safely.
     */
    private fun copyResourceDirToTemp(): File {
        val tempDir = createTempDirectory("rename-test-").toFile()
        val targetRoot = File(tempDir, "tmp").apply { mkdirs() }

        val reorderFilesUrl = requireNotNull(
            RenameUtilsTest::class.java.classLoader.getResource("reorder-files")
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

    private fun getDataDirs(baseDir: File, folderName: String): Pair<File, File> {
        val access = File(baseDir, "$folderName/representations/access/data")
        val primary = File(baseDir, "$folderName/representations/primary/data")
        return access to primary
    }

    /** Creates a file with content in the expected directory structure under tempRoot */
    private fun createFile(itemId: String, repType: String, filename: String, content: String = "data"): File {
        val dir = File(tempRoot, "$itemId/representations/$repType/data").apply { mkdirs() }
        return File(dir, filename).apply { writeText(content) }
    }

    @Test
    fun `renameAll using renameInstructions`() {
        renameInstructions.forEach { instruction ->
            val folderName = instruction.originalName.substringBeforeLast('_')
            val (accessDir, primaryDir) = getDataDirs(tempRoot, folderName)
            assertTrue(File(accessDir, instruction.originalName).exists())
            assertTrue(File(primaryDir, instruction.originalName).exists())
        }
        renameAll(tempRoot.toPath(), renameInstructions)

        val expectedByItem = renameInstructions
            .groupBy { it.newName.substringBeforeLast('_') }
            .mapValues { (_, instructions) -> instructions.map { it.newName }.toSet() }

        expectedByItem.forEach { (itemId, expectedNames) ->
            val (accessDir, primaryDir) = getDataDirs(tempRoot, itemId)
            assertEquals(expectedNames, accessDir.listFiles()?.map { it.name }?.toSet().orEmpty())
            assertEquals(expectedNames, primaryDir.listFiles()?.map { it.name }?.toSet().orEmpty())
        }
    }

    @Test
    fun `renameAll does nothing when originalName equals newName`() {
        // Get similar instructions
        val similarInstructions: List<RenameInstruction> = renameInstructions.filter { it.originalName == it.newName }

        // Capture pre-state
        val preState = tempRoot.walkTopDown()
            .filter { it.isFile }
            .map { it.relativeTo(tempRoot).path }
            .toSet()

        renameAll(tempRoot.toPath(), similarInstructions)

        // Capture post-state
        val postState = tempRoot.walkTopDown()
            .filter { it.isFile }
            .map { it.relativeTo(tempRoot).path }
            .toSet()

        assertEquals(preState, postState, "No files should be moved or renamed")
    }

    @Test
    fun `renameAll cleans up temporary directory on success`() {
        renameAll(tempRoot.toPath(), renameInstructions)

        // Assert no temp_conflicts_ directory remains
        val tempDirs = tempRoot.listFiles { file ->
            file.isDirectory && file.name.startsWith("temp_conflicts_")
        }
        assertTrue(tempDirs.isNullOrEmpty(), "Temporary directory should be cleaned up")
    }

    @Test
    fun `renameAll rolls back all files when one instruction fails`() {
        val itemId = "tekst_test123"
        val accessFile = createFile(itemId, "access", "${itemId}_00001.jp2", "access-content")
        val primaryFile = createFile(itemId, "primary", "${itemId}_00001.jp2", "primary-content")

        val validInstruction = RenameInstruction(
            originalName = "${itemId}_00001.jp2",
            newName = "${itemId}_00002.jp2"
        )
        val invalidInstruction = RenameInstruction(
            originalName = "invalid-original",
            newName = "invalid-new"
        )

        assertThrows(IllegalArgumentException::class.java) {
            renameAll(tempRoot.toPath(), listOf(validInstruction, invalidInstruction))
        }

        // Original files should be restored
        assertTrue(accessFile.exists(), "Access file should be rolled back")
        assertTrue(primaryFile.exists(), "Primary file should be rolled back")
        assertEquals("access-content", accessFile.readText(), "Access file content should be intact")
        assertEquals("primary-content", primaryFile.readText(), "Primary file content should be intact")

        // Renamed files should NOT exist
        assertFalse(File(accessFile.parent, "${itemId}_00002.jp2").exists(), "Renamed access file should not exist")
        assertFalse(File(primaryFile.parent, "${itemId}_00002.jp2").exists(), "Renamed primary file should not exist")
    }

    @Test
    fun `renameAll cleans up temporary directory on rollback`() {
        val invalidInstruction = RenameInstruction(
            originalName = "invalid-original",
            newName = "invalid-new"
        )

        assertThrows(IllegalArgumentException::class.java) {
            renameAll(tempRoot.toPath(), listOf(invalidInstruction))
        }

        val tempDirs = tempRoot.listFiles { file ->
            file.isDirectory && file.name.startsWith("temp_conflicts_")
        }
        assertTrue(tempDirs.isNullOrEmpty(), "Temporary directory should be cleaned up after rollback")
    }

    @Test
    fun `renameAll throws on path traversal attempt`() {
        assertThrows(IllegalArgumentException::class.java) {
            renameAll(
                tempRoot.toPath(), listOf(
                    RenameInstruction(
                        originalName = "tekst_abc123_00001.jp2",
                        newName = "../outside_00001.jp2"
                    )
                )
            )
        }
    }
}