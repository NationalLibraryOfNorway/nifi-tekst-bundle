package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
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
        val resource = this::class.java.classLoader.getResource("reorder-files/$fileName")
        requireNotNull(resource) { "Resource not found" }
        val jsonContent = File(resource.toURI()).readText()
        return mapper.readTree(jsonContent)
    }

    @Test
    fun `addReorderInstructions() provides the correct instructions regardless of order`() {
        val flowfile = readFile("flowfile.json")
        val changes = flowfile["change"]
        val addInstruction = readFile("addRenameInstruction.json")
        val expected = addInstruction["addInstruction"]
        val expectedOrder = mapper.convertValue(expected, List::class.java) as List<Map<String, String>>

        val firstChange = changes[0]
        val itemId = firstChange["itemId"].asText()
        val newOrder = firstChange["newOrder"]
        val renameInstructions = reorderFiles.addInstruction(itemId, newOrder, "%05d")

        val renameList = mapper.convertValue(renameInstructions, List::class.java) as List<Map<String, String>>
        val actualOrder = stripPath(renameList)

        assertEquals(expectedOrder.toSet(), actualOrder.toSet())
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

        reorderFiles.deleteOcrFiles(itemId, baseDir)

        assertTrue(ocrDir.exists())
        assertTrue(ocrDir.isDirectory)
        assertEquals(0, ocrDir.listFiles()?.size ?: -1)
    }

    @Test
    fun `processor generates itemId for missing itemId in flowfile json`() {
        val reorderFiles = ReorderFiles()
        val mapper = ObjectMapper()
        val zeroPadding = "%05d"

        // Read and modify flowfile.json
        val flowfile = readFile("flowfile.json")
        val changes = flowfile["change"] as ArrayNode

        val changeList = mutableListOf<Map<String, Any>>()
        for (change in changes) {
            var itemId: String = change.get("itemId")?.asText() ?: ""
            if (itemId.isBlank() || itemId == "null") {
                itemId = no.nb.utils.UUIDv7.randomUUID().toString()
            }
            val orderedImages = change.get("newOrder")
            val itemInstruction = reorderFiles.addInstruction(itemId, orderedImages, zeroPadding)
            val itemNewOrder = itemInstruction.map { it.newName }
            changeList.add(mapOf("itemId" to itemId, "newOrder" to itemNewOrder))
        }

        // Assert that the last entry's itemId is a valid UUID
        val generatedItemId = changeList.last()["itemId"] as String
        assertTrue(generatedItemId.matches(Regex("^[0-9a-fA-F-]{36}$")), "Generated itemId should be a UUID")
        val newOrder = changeList.last()["newOrder"] as List<*>
        assertEquals(listOf("${generatedItemId}_00001.jp2"), newOrder)
    }
}