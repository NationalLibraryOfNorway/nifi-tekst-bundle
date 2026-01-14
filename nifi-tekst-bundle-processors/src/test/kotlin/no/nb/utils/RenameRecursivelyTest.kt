package no.nb.utils

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import no.nb.models.Entry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.io.path.createTempDirectory

object RenameAllManualTest {
    private val mapper = ObjectMapper()

    //Json file reader
    private fun readFile(fileName: String): JsonNode {
        val resource = this::class.java.classLoader.getResource("reorder-files/$fileName.json")
        requireNotNull(resource) { "Resource not found" }
        val jsonContent = File(resource.toURI()).readText()
        return mapper.readTree(jsonContent)
    }

    /**
     * Copies `src/test/resources/reorder-files` into a temp directory
     * so files can be renamed safely.
     */
    private fun copyResourceDirToTemp(): File {
        val tempDir = createTempDirectory("rename-test-").toFile()
        val targetRoot = File(tempDir, "tmp").apply { mkdirs() }

        // Find the reorder-files resource directory
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

    private val flowFile = readFile("flowfile")
    private val itemId = flowFile["change"][0]["itemId"].asText()

    private val addEntries = readFile("addEntries")
    private val firstEntry = addEntries["firstEntry"]

    private fun getDataDir(baseDir: File, itemId: String): Pair<File, File> {
        val access = File(baseDir, "$itemId/representations/access/data")
        val primary = File(baseDir, "$itemId/representations/primary/data")
        return Pair(access, primary)
    }

    @Test
    fun `renameAll`() {
        val tempRoot = copyResourceDirToTemp()
        val entries = firstEntry.map { node ->
            Entry(
                originalName = node["originalName"].asText(), newName = node["newName"].asText()
            )
        }

        // Pre-check: files exist and contain expected originalName
        for (entry in entries) {
            val originalId = entry.originalName.substringBefore('_')
            val (accessDir, primaryDataDir) = getDataDir(tempRoot, originalId)
            val accessFile = File(accessDir, entry.originalName)
            val primaryFile = File(primaryDataDir, entry.originalName)

            assertTrue(accessFile.exists(), "Access file ${entry.originalName} should exist before renaming")
            assertTrue(primaryFile.exists(), "Primary file ${entry.originalName} should exist before renaming")
        }

        renameAll(
            entries = entries, baseDir = tempRoot
        )

        // Check that files have been renamed correctly
        for (entry in entries) {
            val (accessDir, primaryDataDir) = getDataDir(tempRoot, itemId)
            accessDir.listFiles { f -> f.extension == "jp2" && f.name == entry.newName }?.forEach { file ->
                assertEquals(entry.newName, file.name.trim())
            }
            primaryDataDir.listFiles { f -> f.extension == "jp2" && f.name == entry.newName }?.forEach { file ->
                assertEquals(entry.newName, file.name.trim())
            }
        }

        // Cleanup
        tempRoot.deleteRecursively()
    }

    @Test
    fun `renameRecursively renames both access and primary files`() {
        val tempDir = createTempDir()
        val originalName = "${itemId}_00002.jpg"
        val newName = "${itemId}_00001.jpg"
        val entries = listOf(Entry(originalName, newName))
        val visited = mutableSetOf<Int>()

        // Create access and primary directories and files
        val (accessDir, primaryDir) = getDataDir(tempDir, itemId)
        accessDir.mkdirs()
        primaryDir.mkdirs()
        val accessFile = File(accessDir, originalName).apply { writeText("access content") }
        val primaryFile = File(primaryDir, originalName).apply { writeText("primary content") }
        val accessTarget = File(accessDir, newName)
        val primaryTarget = File(primaryDir, newName)

        // Pre-check: files exist and the target does not
        assertTrue(accessFile.exists())
        assertTrue(primaryFile.exists())
        assertFalse(accessTarget.exists())
        assertFalse(primaryTarget.exists())

        // Act: call renameRecursively directly
        renameRecursively(
            index = 0,
            entries = entries,
            baseDir = tempDir,
            visited = visited,
            tempDir = tempDir
        )

        // Post-check: files have been renamed and the file has the expected content
        assertFalse(accessFile.exists())
        assertFalse(primaryFile.exists())
        assertTrue(accessTarget.exists())
        assertTrue(primaryTarget.exists())
        assertEquals("access content", accessTarget.readText())
        assertEquals("primary content", primaryTarget.readText())

        tempDir.deleteRecursively()
    }

    @Test
    fun `getAccessAndPrimary returns correct files or nulls`() {
        val tempDir = createTempDir()
        val fileName = "${itemId}_00001.jpg"

        // Create access and primary directories
        val (accessDir, primaryDir) = getDataDir(tempDir, itemId)
        accessDir.mkdirs()
        primaryDir.mkdirs()

        // Create only the access file
        val accessFile = File(accessDir, fileName)
        accessFile.writeText("access content")

        // Do NOT create the primary file to test null case
        val result = getAccessAndPrimary(tempDir, fileName, tempDir)

        assertEquals(accessFile, result.access)
        assertEquals(null, result.primary)

        // Now create the primary file and test again
        val primaryFile = File(primaryDir, fileName)
        primaryFile.writeText("primary content")

        val result2 = getAccessAndPrimary(tempDir, fileName, tempDir)
        assertEquals(accessFile, result2.access)
        assertEquals(primaryFile, result2.primary)

        // Clean up
        tempDir.deleteRecursively()
    }
}
