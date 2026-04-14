package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import no.nb.utils.TestFileUtils
import no.nb.utils.UUIDv7
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Path
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.BeforeEach
import java.nio.file.Files
import kotlin.test.assertNotNull

class ReorderFilesTest {
    private val reorderFiles = ReorderFiles()
    private val mapper = ObjectMapper()
    private lateinit var runner: TestRunner
    private lateinit var flowFile: JsonNode
    private lateinit var changes: JsonNode
    val baseDir: Path = Files.createTempDirectory("reorder-files-test").normalize()

    private fun stripPath(entries: List<Map<String, String>>): List<Map<String, String>> =
        entries.map { entry -> entry.mapValues { (_, v) -> v.substringAfterLast('/') } }

    @BeforeEach
    fun setUp() {
        flowFile = TestFileUtils.readJson("flowfile.json")
        changes = flowFile["changes"]
        runner = TestRunners.newTestRunner(ReorderFiles())
        runner.setProperty(ReorderFiles.BASE_DIR, baseDir.toString())
        runner.assertValid()
    }

    @AfterEach
    fun tearDown() {
        baseDir.toFile().deleteRecursively()
    }

    @Test
    fun `ReorderFiles processor should move and rename files correctly`() {
        val fixedUuid = "uuid-1234"
        val reorderFilesWithUuid = ReorderFiles(uuidProvider = { fixedUuid })
        val inputJson = mapper.writeValueAsString(flowFile)
        val createdOcrFiles = mutableListOf<File>()

        // Create the initial directory structure and files
        fun createTestFilesForChange(change: JsonNode) {
            val orderedImages = change["orderedImageIds"] ?: return
            orderedImages.forEach { imageName ->
                val rawName = imageName.asText()
                val fileName = if (rawName.endsWith(".jp2")) rawName else "$rawName.jp2"
                val accessFile = TestFileUtils.createFile(baseDir, "access", fileName)
                val primaryFile = TestFileUtils.createFile(baseDir, "primary", fileName)

                // Create ocr xml for each access file
                val itemId = change["itemId"]?.asText() ?: fixedUuid
                val folderName = "tekst_$itemId"
                val ocrDir = baseDir.resolve("$folderName/representations/access/metadata/other/ocr").toFile()
                ocrDir.mkdirs()
                val xmlFile = File(ocrDir, "${rawName}.xml")
                xmlFile.writeText("<xml>dummy</xml>")

                //Verify that files are created
                assertTrue(accessFile.exists(), "File should exist: ${accessFile.absolutePath}")
                assertTrue(primaryFile.exists(), "File should exist: ${primaryFile.absolutePath}")
                assertTrue(xmlFile.exists(), "OCR XML should exist: ${xmlFile.absolutePath}")
                createdOcrFiles += xmlFile
            }
        }

        changes.forEach { createTestFilesForChange(it) }

        //Run the processor
        runner = TestRunners.newTestRunner(reorderFilesWithUuid)
        runner.setProperty(ReorderFiles.BASE_DIR, baseDir.toString())
        runner.assertValid()
        runner.enqueue(inputJson.toByteArray())
        runner.run()
        runner.assertAllFlowFilesTransferred(ReorderFiles.REL_SUCCESS, 1)

        // Verify the output
        val outFlowFile = runner.getFlowFilesForRelationship(ReorderFiles.REL_SUCCESS)[0]
        val outputJson = mapper.readTree(String(outFlowFile.toByteArray()))
        val items = outputJson["items"]
        assertNotNull(items, "Output should contain items array")
        assertEquals(changes.size(), items.size(), "Should have one item per change")

        val topLevelFields = listOf("batchId", "font", "materialType", "publicationType", "language", "digital")
        for (field in topLevelFields) {
            if (flowFile[field]?.isBoolean == true) {
                assertEquals(flowFile[field]?.asBoolean(), outputJson[field]?.asBoolean(), "Field '$field' should match")
            } else {
                assertEquals(flowFile[field]?.asText(), outputJson[field]?.asText(), "Field '$field' should match")
            }
        }

        items.forEachIndexed { index, item ->
            val change = changes[index]
            val expectedItemId = change["itemId"]?.asText()
            val expectedPages = change["orderedImageIds"]?.size() ?: 0

            //The last item in flowFile is a new item, so a new UUID is generated, in this case a mocked value
            if (expectedItemId != null) {
                assertEquals(expectedItemId, item["itemId"]?.asText(), "itemId should match for item $index")
            } else {
                assertEquals(fixedUuid, item["itemId"]?.asText(), "itemId should match for item $index")
            }
            assertEquals(expectedPages, item["pages"]?.asInt(), "page count should match for item $index")
        }

        //Check that all files has been moved and renamed after the processor has ran
        val resultFile = File("src/test/resources/reorder-files/reorderedPaths.txt")
        val expectedPaths = resultFile.readLines().map { baseDir.resolve(it).normalize().toFile() }
        expectedPaths.forEach { file ->
            assertTrue(file.exists(), "Expected file does not exist: $file")
        }

        // Verify all ocr xml files have been deleted, but ocr folder exists
        createdOcrFiles.forEach { xmlFile ->
            assertTrue(!xmlFile.exists(), "OCR XML should be deleted: ${xmlFile.absolutePath}")
            val ocrDir = xmlFile.parentFile
            assertTrue(ocrDir.exists() && ocrDir.isDirectory, "OCR dir should still exist: ${ocrDir.absolutePath}")
        }
    }

    @Test
    fun `addReorderInstructions() provides the correct instructions regardless of order`() {
        val addInstruction = TestFileUtils.readJson("addRenameInstruction.json")
        val expected = addInstruction["addInstruction"]
        val expectedOrder = mapper.convertValue(expected, List::class.java) as List<*>

        val firstChange = changes[0]
        val itemId = firstChange["itemId"].asText()
        val newOrder = firstChange["orderedImageIds"]
        val renameInstructions = reorderFiles.addInstruction(itemId, newOrder, "%05d", baseDir)
        val renameList = mapper.convertValue(renameInstructions, List::class.java) as List<Map<String, String>>
        val actualOrder = stripPath(renameList)

        assertEquals(expectedOrder.toSet(), actualOrder.toSet())
    }

    @Test
    fun `deleteOcr removes all files in ocr folder but keeps the folder`() {
        val itemId = UUIDv7.randomUUID().toString()
        val folderName = "tekst_$itemId"
        val ocrDir = baseDir.resolve("$folderName/representations/access/metadata/other/ocr").toFile()
        ocrDir.mkdirs()
        val file1 = File(ocrDir, "${folderName}_00001.xml").apply { writeText("text1") }
        val file2 = File(ocrDir, "${folderName}_00002.xml").apply { writeText("text2") }

        assertTrue(file1.exists())
        assertTrue(file2.exists())

        reorderFiles.deleteOcrFiles(itemId, baseDir)

        assertTrue(ocrDir.exists())
        assertTrue(ocrDir.isDirectory)
        assertEquals(0, ocrDir.listFiles()?.size ?: -1)
    }

    @Test
    fun `processor generates itemId for missing itemId in flowfile json`() {
        val zeroPadding = "%05d"

        val changeList = mutableListOf<Map<String, Any>>()
        for (change in changes) {
            var itemId: String = change.get("itemId")?.asText() ?: ""
            if (itemId.isBlank() || itemId == "null") {
                itemId = UUIDv7.randomUUID().toString()
            }
            val orderedImages = change.get("orderedImageIds")
            val itemInstruction = reorderFiles.addInstruction(itemId, orderedImages, zeroPadding, baseDir)
            val itemNewOrder = itemInstruction.map { it.newName }
            changeList.add(mapOf("itemId" to itemId, "orderedImageIds" to itemNewOrder))
        }

        // Assert that the last entry's itemId is a valid UUID
        val generatedItemId = changeList.last()["itemId"] as String
        assertTrue(
            generatedItemId.matches(Regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")),
            "Generated itemId should be a valid UUID"
        )
        val newOrder = changeList.last()["orderedImageIds"] as List<*>
        assertEquals(listOf("tekst_${generatedItemId}_00001.jp2"), newOrder)
    }
}