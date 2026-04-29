package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.minio.MinioClient
import no.nb.utils.MinIOTestBase
import no.nb.utils.TestFileUtils
import no.nb.utils.TestFileUtils.createDiskFiles
import no.nb.utils.TestFileUtils.createOcrFiles
import no.nb.utils.TestFileUtils.createS3Files
import no.nb.utils.UUIDv7
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

class ReorderFilesTest : MinIOTestBase() {
    private val reorderFiles = ReorderFiles()
    private val mapper = ObjectMapper()
    private lateinit var runner: TestRunner
    private lateinit var flowFile: JsonNode
    private lateinit var changes: JsonNode
    val baseDir: Path = Files.createTempDirectory("reorder-files-test").normalize()
    val fixedUuid = "uuid-1234"
    val testPrefix = "NEWSPAPER"

    @BeforeEach
    fun setUp() {
        flowFile = TestFileUtils.readJson("flowfile.json")
        changes = flowFile["changes"]
        runner = setupTestRunner()
    }

    @AfterEach
    fun tearDown() {
        baseDir.toFile().deleteRecursively()
    }

    private fun setupTestRunner(processor: ReorderFiles = reorderFiles): TestRunner =
        TestRunners.newTestRunner(processor).apply {
            setProperty(ReorderFiles.BASE_DIR, baseDir.toString())
            setProperty(ReorderFiles.ENDPOINT, s3Endpoint)
            setProperty(ReorderFiles.BUCKET, BUCKET)
            setProperty(ReorderFiles.ACCESS_KEY, s3AccessKey)
            setProperty(ReorderFiles.SECRET_KEY, s3SecretKey)
            setProperty(ReorderFiles.REGION, REGION)
            setProperty(ReorderFiles.PREFIX, testPrefix)
            assertValid()
        }

    private fun stripPath(entries: List<Map<String, String>>): List<Map<String, String>> =
        entries.map { entry -> entry.mapValues { (_, v) -> v.substringAfterLast('/') } }

    private val expectedStructure = listOf(
        "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec/representations/access/data/tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00001.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec/representations/primary/data/tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00001.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec/representations/access/data/tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00002.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec/representations/primary/data/tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00002.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec/representations/access/data/tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00003.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec/representations/primary/data/tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00003.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec/representations/access/data/tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00004.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec/representations/primary/data/tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00004.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec/representations/access/data/tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00005.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec/representations/primary/data/tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00005.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5dfe09e6ec1c/representations/access/data/tekst_019a3aa3-d0af-7658-9a44-5dfe09e6ec1c_00001.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5dfe09e6ec1c/representations/primary/data/tekst_019a3aa3-d0af-7658-9a44-5dfe09e6ec1c_00001.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5dfe09e6ec1c/representations/access/data/tekst_019a3aa3-d0af-7658-9a44-5dfe09e6ec1c_00002.tif",
        "tekst_019a3aa3-d0af-7658-9a44-5dfe09e6ec1c/representations/primary/data/tekst_019a3aa3-d0af-7658-9a44-5dfe09e6ec1c_00002.tif",
        "tekst_uuid-1234/representations/access/data/tekst_uuid-1234_00001.tif",
        "tekst_uuid-1234/representations/primary/data/tekst_uuid-1234_00001.tif"
    )

    private fun verifyOutputJson() {
        val outFlowFile = runner.getFlowFilesForRelationship(ReorderFiles.REL_SUCCESS)[0]
        val outputJson = mapper.readTree(String(outFlowFile.toByteArray()))
        val items = outputJson["items"]

        assertNotNull(items, "Output should contain items array")
        assertEquals(changes.size(), items.size(), "Should have one item per change")

        listOf("batchId", "font", "materialType", "publicationType", "language", "digital").forEach { field ->
            if (flowFile[field]?.isBoolean == true) {
                assertEquals(
                    flowFile[field]?.asBoolean(), outputJson[field]?.asBoolean(), "Field '$field' should match"
                )
            } else {
                assertEquals(flowFile[field]?.asText(), outputJson[field]?.asText(), "Field '$field' should match")
            }
        }

        items.forEachIndexed { index, item ->
            val change = changes[index]
            val expectedItemId = change["itemId"]?.asText()
            val expectedPages = change["orderedImageIds"]?.size() ?: 0
            val actualItemId = if (expectedItemId != null) expectedItemId else fixedUuid
            assertEquals(actualItemId, item["itemId"]?.asText(), "itemId should match for item $index")
            assertEquals(expectedPages, item["pages"]?.asInt(), "page count should match for item $index")
        }
    }

    private fun verifyDiskStructure() {
        expectedStructure.forEach { relativePath ->
            val file = baseDir.resolve(relativePath).toFile()
            assertTrue(file.exists(), "Expected disk file does not exist after rename: ${file.absolutePath}")
        }
    }

    private fun verifyS3Structure(createdS3Keys: List<String>) {
        val expectedS3Keys = expectedStructure.map { "${testPrefix}/$it" }
        expectedS3Keys.forEach { s3Key ->
            assertTrue(keyExists(s3Key), "Expected S3 key does not exist after rename: $s3Key")
        }
        createdS3Keys.filter { oldKey -> oldKey !in expectedS3Keys }.forEach { oldKey ->
                assertFalse(keyExists(oldKey), "Old S3 key should have been deleted after rename: $oldKey")
            }
        assertTrue(
            listAllKeys().none { it.startsWith("tmp_") }, "No temp keys should remain after successful run"
        )
    }

    private fun verifyOcrFilesDeleted(createdOcrFiles: List<File>) {
        createdOcrFiles.forEach { xmlFile ->
            assertFalse(xmlFile.exists(), "OCR XML should be deleted: ${xmlFile.absolutePath}")
            assertTrue(
                xmlFile.parentFile.exists() && xmlFile.parentFile.isDirectory,
                "OCR directory should still exist: ${xmlFile.parentFile.absolutePath}"
            )
        }
    }

    @Test
    fun `processor should move and rename files correctly on disk and S3`() {
        val reorderFilesWithUuid = ReorderFiles(uuidProvider = { fixedUuid })
        val inputJson = mapper.writeValueAsString(flowFile)

        createDiskFiles(changes, baseDir)
        val ocrFiles = createOcrFiles(changes, baseDir, fixedUuid)
        val s3Keys = createS3Files(changes, this, testPrefix)

        runner = setupTestRunner(reorderFilesWithUuid)
        runner.enqueue(inputJson)
        runner.run()
        runner.assertAllFlowFilesTransferred(ReorderFiles.REL_SUCCESS, 1)
        verifyOutputJson()
        verifyDiskStructure()
        verifyS3Structure(s3Keys)
        verifyOcrFilesDeleted(ocrFiles)
    }

    @Test
    fun `addInstruction provides correct rename instructions regardless of order`() {
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
    fun `deleteOcrFiles removes all xml files in ocr folder but keeps the folder`() {
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
    fun `processor generates a valid uuid for items missing itemId in flowfile`() {
        val changeList = mutableListOf<Map<String, Any>>()
        for (change in changes) {
            var itemId = change.get("itemId")?.asText() ?: ""
            if (itemId.isBlank() || itemId == "null") itemId = UUIDv7.randomUUID().toString()

            val orderedImages = change.get("orderedImageIds")
            val itemInstruction = reorderFiles.addInstruction(itemId, orderedImages, "%05d", baseDir)
            changeList.add(mapOf("itemId" to itemId, "orderedImageIds" to itemInstruction.map { it.newName }))
        }

        val generatedItemId = changeList.last()["itemId"] as String
        assertTrue(
            generatedItemId.matches(Regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")),
            "Generated itemId should be a valid UUID"
        )
        val newOrder = changeList.last()["orderedImageIds"] as List<*>
        assertEquals(listOf("tekst_${generatedItemId}_00001.tif"), newOrder)
    }

    @Test
    fun `processor should rollback disk changes when s3 rename fails`() {
        val mockS3Client = mock(MinioClient::class.java)
        `when`(mockS3Client.copyObject(any(io.minio.CopyObjectArgs::class.java))).thenThrow(RuntimeException("Simulated S3 failure"))

        val reorderFilesWithUuid =
            ReorderFiles(uuidProvider = { fixedUuid }, s3ClientProvider = { _, _, _, _ -> mockS3Client })

        val inputJson = mapper.writeValueAsString(flowFile)

        val diskFiles = createDiskFiles(changes, baseDir)
        val ocrFiles = createOcrFiles(changes, baseDir, fixedUuid)

        val failingRunner = TestRunners.newTestRunner(reorderFilesWithUuid).apply {
            setProperty(ReorderFiles.BASE_DIR, baseDir.toString())
            setProperty(ReorderFiles.ENDPOINT, s3Endpoint)
            setProperty(ReorderFiles.BUCKET, BUCKET)
            setProperty(ReorderFiles.ACCESS_KEY, s3AccessKey)
            setProperty(ReorderFiles.SECRET_KEY, s3SecretKey)
            setProperty(ReorderFiles.REGION, REGION)
            setProperty(ReorderFiles.PREFIX, testPrefix)
            assertValid()
        }
        failingRunner.enqueue(inputJson.toByteArray())
        failingRunner.run()

        failingRunner.assertAllFlowFilesTransferred(ReorderFiles.REL_FAILURE, 1)

        diskFiles.forEach { file ->
            assertTrue(file.exists(), "Original disk file should be restored after rollback: ${file.absolutePath}")
        }
        ocrFiles.forEach { file ->
            assertTrue(file.exists(), "OCR file should still exist since S3 rename failed: ${file.absolutePath}")
        }
    }
}