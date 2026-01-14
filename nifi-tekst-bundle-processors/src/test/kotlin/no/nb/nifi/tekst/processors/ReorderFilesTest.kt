package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File

class ReorderFilesTest {
    private val reorderFiles = ReorderFiles()
    private val mapper = ObjectMapper()

    private fun stripPath(entries: List<Map<String, String>>): List<Map<String, String>> =
        entries.map { entry -> entry.mapValues { (_, v) -> v.substringAfterLast('/') } }

    private fun readFile(fileName: String): JsonNode {
        val resource = this::class.java.classLoader.getResource("reorder-files/$fileName.json")
        requireNotNull(resource) { "Resource not found" }
        val jsonContent = File(resource.toURI()).readText()
        return mapper.readTree(jsonContent)
    }

    @Test
    fun `addEntries() provides the correct order of entries`() {
        val flowfile = readFile("flowfile")
        val changes = flowfile["change"]
        val addEntries = readFile("addEntries")

        val firstChange = changes[0]
        val itemId = firstChange["itemId"].asText()
        val order = firstChange["newOrder"]
        val actualEntry = reorderFiles.addEntries(itemId, order, "%05d")
        val expected = addEntries["firstEntry"]

        val actualEntryList = mapper.convertValue(actualEntry, List::class.java) as List<Map<String, String>>
        val actualEntryStripped = stripPath(actualEntryList)
        val actual = mapper.valueToTree<JsonNode>(actualEntryStripped)

        assertEquals(expected, actual)
    }

    @Test
    fun `deleteOcr removes all files in ocr folder but keeps the folder`() {
        val itemId = "testItem"
        val baseDir = createTempDir()
        val ocrDir = File(baseDir, "$itemId/access/metadata/other/ocr")
        ocrDir.mkdirs()
        val file1 = File(ocrDir, "ocr1.xml").apply { writeText("text1") }
        val file2 = File(ocrDir, "ocr2.xml").apply { writeText("text2") }

        assertTrue(file1.exists())
        assertTrue(file2.exists())

        reorderFiles.deleteOcr(itemId, baseDir)

        assertTrue(ocrDir.exists())
        assertTrue(ocrDir.isDirectory)
        assertEquals(0, ocrDir.listFiles()?.size ?: -1)
    }
}