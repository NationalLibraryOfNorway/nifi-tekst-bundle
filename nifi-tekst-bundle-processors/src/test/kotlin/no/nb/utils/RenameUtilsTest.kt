package no.nb.utils

import com.fasterxml.jackson.databind.JsonNode
import no.nb. models.RenameInstruction
import no.nb.utils.RenameUtils.renameAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Path
import kotlin.io.path.createTempDirectory

class RenameUtilsTest {

    private lateinit var jsonFile: JsonNode
    private lateinit var baseDir: Path
    private lateinit var baseDirFile: File


    private val renameInstructions: List<RenameInstruction>
        get() = jsonFile["renameInstructions"].map { node ->
            RenameInstruction(
                originalName = node["originalName"].asText(),
                newName = node["newName"].asText()
            )
        }

    @BeforeEach
    fun setUp() {
        jsonFile = TestFileUtils.readFile("renameInstructions.json")
        baseDir = copyResourceDirToTemp()
        baseDirFile = baseDir.toFile()
    }

    @AfterEach
    fun tearDown() {
        baseDir.toFile().deleteRecursively()
    }

    /**
     * Copies `src/test/resources/reorder-files` into a temp directory
     * so files can be renamed safely.
     */
    private fun copyResourceDirToTemp(): Path {
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
        return targetRoot.toPath()
    }

    private fun getDataDirs(folderName: String): Pair<File, File> {
        val access = baseDir.resolve("$folderName/representations/access/data").toFile()
        val primary = baseDir.resolve("$folderName/representations/primary/data").toFile()
        return access to primary
    }

    @Test
    fun `renameAll using renameInstructions`() {
        renameInstructions.forEach { instruction ->
            val folderName = instruction.originalName.substringBeforeLast('_')
            val (accessDir, primaryDir) = getDataDirs(folderName)
            assertTrue(File(accessDir, instruction.originalName).exists())
            assertTrue(File(primaryDir, instruction.originalName).exists())
        }
        renameAll(baseDir, renameInstructions)

        val expectedByItem = renameInstructions
            .groupBy { it.newName.substringBeforeLast('_') }
            .mapValues { (_, instructions) -> instructions.map { it.newName }.toSet() }

        expectedByItem.forEach { (itemId, expectedNames) ->
            val (accessDir, primaryDir) = getDataDirs(itemId)
            assertEquals(expectedNames, accessDir.listFiles()?.map { it.name }?.toSet().orEmpty())
            assertEquals(expectedNames, primaryDir.listFiles()?.map { it.name }?.toSet().orEmpty())
        }
    }

    @Test
    fun `renameAll does nothing when originalName equals newName`() {
        // Get similar instructions
        val similarInstructions: List<RenameInstruction> = renameInstructions.filter { it.originalName == it.newName }

        // Capture pre-state
        val preState = baseDirFile.walkTopDown()
            .filter { it.isFile }
            .map { it.relativeTo(baseDirFile).path }
            .toSet()

        renameAll(baseDir, similarInstructions)

        // Capture post-state
        val postState = baseDirFile.walkTopDown()
            .filter { it.isFile }
            .map { it.relativeTo(baseDirFile).path }
            .toSet()

        assertEquals(preState, postState, "No files should be moved or renamed")
    }

    @Test
    fun `renameAll cleans up temporary directory on success`() {
        renameAll(baseDir, renameInstructions)

        // Assert no temp_conflicts_ directory remains
        val tempDirs = baseDirFile.listFiles { file ->
            file.isDirectory && file.name.startsWith("temp_conflicts_")
        }
        assertTrue(tempDirs.isNullOrEmpty(), "Temporary directory should be cleaned up")
    }

    @Test
    fun `renameAll rolls back all files when one instruction fails`() {
        val itemId = "tekst_test123"
        val accessFile = TestFileUtils.createFile(baseDir, itemId, "access", "${itemId}_00001.jp2", "access-content")
        val primaryFile = TestFileUtils.createFile(baseDir, itemId, "primary", "${itemId}_00001.jp2", "primary-content")

        val validInstruction = RenameInstruction(
            originalName = "${itemId}_00001.jp2",
            newName = "${itemId}_00002.jp2"
        )
        val invalidInstruction = RenameInstruction(
            originalName = "invalid-original",
            newName = "invalid-new"
        )

        assertThrows(IllegalArgumentException::class.java) {
            renameAll(baseDir, listOf(validInstruction, invalidInstruction))
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
            renameAll(baseDir, listOf(invalidInstruction))
        }

        val tempDirs = baseDirFile.listFiles { file ->
            file.isDirectory && file.name.startsWith("temp_conflicts_")
        }
        assertTrue(tempDirs.isNullOrEmpty(), "Temporary directory should be cleaned up after rollback")
    }

    @Test
    fun `renameAll throws on path traversal attempt`() {
        assertThrows(IllegalArgumentException::class.java) {
            renameAll(
                baseDir, listOf(
                    RenameInstruction(
                        originalName = "tekst_abc123_00001.jp2",
                        newName = "../outside_00001.jp2"
                    )
                )
            )
        }
    }
}