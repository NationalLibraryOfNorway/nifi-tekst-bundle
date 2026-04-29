package no.nb.utils

import com.fasterxml.jackson.databind.JsonNode
import no.nb. models.RenameInstruction
import no.nb.utils.RenameDiskUtils.renameFilesOnDisk
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Path
import kotlin.io.path.createTempDirectory

class RenameDiskUtilsTest: MinIOTestBase() {

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
        jsonFile = TestFileUtils.readJson("renameInstructions.json")
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
            RenameDiskUtilsTest::class.java.classLoader.getResource("reorder-files")
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
    fun `renameFilesOnDisk using renameInstructions`() {
        // Create the initial directory structure and files based on the renameInstructions
        renameInstructions.forEach { instruction ->
            val accessFile = TestFileUtils.createFile(baseDir, "access", instruction.originalName, "access-content")
            val primaryFile = TestFileUtils.createFile(baseDir, "primary", instruction.originalName, "primary-content")

            //Verifying that the files are created
            assertTrue(accessFile.exists(), "Access file should exist before rename")
            assertTrue(primaryFile.exists(), "Primary file should exist before rename")
        }

        //Run the renamAll function with the renameInstructions
        renameFilesOnDisk(baseDir, renameInstructions)

        //Group the new filenames key, example folderName: tekst_123, value: array of fileNames: [tekst_123_001.jp2, tekst_123_002.jp2]
        val newFileNames = renameInstructions
            .groupBy { it.newName.substringBeforeLast('_') }
            .mapValues { (_, instructions) -> instructions.map { it.newName }.toSet() }

        //Verify that the files are renamed
        newFileNames.forEach { (folderName, newFileName) ->
            val (accessDir, primaryDir) = getDataDirs(folderName)
            assertEquals(newFileName, accessDir.listFiles()?.map { it.name }?.toSet().orEmpty())
            assertEquals(newFileName, primaryDir.listFiles()?.map { it.name }?.toSet().orEmpty())
        }
    }

    @Test
    fun `renameFilesOnDisk does nothing when originalName equals newName`() {
        // Get similar instructions
        val similarInstructions: List<RenameInstruction> = renameInstructions.filter { it.originalName == it.newName }

        // Capture pre-state
        val preState = baseDirFile.walkTopDown()
            .filter { it.isFile }
            .map { it.relativeTo(baseDirFile).path }
            .toSet()

        renameFilesOnDisk(baseDir, similarInstructions)

        // Capture post-state
        val postState = baseDirFile.walkTopDown()
            .filter { it.isFile }
            .map { it.relativeTo(baseDirFile).path }
            .toSet()

        assertEquals(preState, postState, "No files should be moved or renamed")
    }

    @Test
    fun `renameFilesOnDisk cleans up temporary directory on success`() {
        renameFilesOnDisk(baseDir, renameInstructions)

        // Assert no temp_conflicts_ directory remains
        val tempDirs = baseDirFile.listFiles { file ->
            file.isDirectory && file.name.startsWith("temp_conflicts_")
        }
        assertTrue(tempDirs.isNullOrEmpty(), "Temporary directory should be cleaned up")
    }

    @Test
    fun `renameFilesOnDisk rolls back all files when one instruction fails`() {
        val folderName = "tekst_test123"
        val accessFile = TestFileUtils.createFile(baseDir,  "access", "${folderName}_00001.jp2", "access-content")
        val primaryFile = TestFileUtils.createFile(baseDir, "primary", "${folderName}_00001.jp2", "primary-content")

        val validInstruction = RenameInstruction(
            originalName = "${folderName}_00001.jp2",
            newName = "${folderName}_00002.jp2"
        )
        val invalidInstruction = RenameInstruction(
            originalName = "invalid-original",
            newName = "invalid-new"
        )

        assertThrows(IllegalArgumentException::class.java) {
            renameFilesOnDisk(baseDir, listOf(validInstruction, invalidInstruction))
        }

        // Original files should be restored
        assertTrue(accessFile.exists(), "Access file should be rolled back")
        assertTrue(primaryFile.exists(), "Primary file should be rolled back")
        assertEquals("access-content", accessFile.readText(), "Access file content should be intact")
        assertEquals("primary-content", primaryFile.readText(), "Primary file content should be intact")

        // Renamed files should NOT exist
        assertFalse(File(accessFile.parent, "${folderName}_00002.jp2").exists(), "Renamed access file should not exist")
        assertFalse(File(primaryFile.parent, "${folderName}_00002.jp2").exists(), "Renamed primary file should not exist")
    }

    @Test
    fun `renameFilesOnDisk cleans up temporary directory on rollback`() {
        val invalidInstruction = RenameInstruction(
            originalName = "invalid-original",
            newName = "invalid-new"
        )

        assertThrows(IllegalArgumentException::class.java) {
            renameFilesOnDisk(baseDir, listOf(invalidInstruction))
        }

        val tempDirs = baseDirFile.listFiles { file ->
            file.isDirectory && file.name.startsWith("temp_conflicts_")
        }
        assertTrue(tempDirs.isNullOrEmpty(), "Temporary directory should be cleaned up after rollback")
    }

    @Test
    fun `renameFilesOnDisk throws on path traversal attempt`() {
        assertThrows(IllegalArgumentException::class.java) {
            renameFilesOnDisk(
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