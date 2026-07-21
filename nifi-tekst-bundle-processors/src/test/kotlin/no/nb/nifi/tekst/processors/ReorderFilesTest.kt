package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.minio.MinioClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.unmockkObject
import no.nb.nifi.tekst.util.S3ClientFactory
import no.nb.utils.S3TestBase
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
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

class ReorderFilesTest : S3TestBase() {
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
        unmockkObject(S3ClientFactory)
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
    fun `deleteDocwizzArtifacts removes all xml files in ocr folder but keeps the folder`() {
        val itemId = UUIDv7.randomUUID().toString()
        val folderName = "tekst_$itemId"
        val ocrDir = baseDir.resolve("$folderName/representations/access/metadata/other/ocr").toFile()
        ocrDir.mkdirs()
        val file1 = File(ocrDir, "${folderName}_00001.xml").apply { writeText("text1") }
        val file2 = File(ocrDir, "${folderName}_00002.xml").apply { writeText("text2") }

        assertTrue(file1.exists())
        assertTrue(file2.exists())

        reorderFiles.deleteDocwizzArtifacts(itemId, baseDir)

        assertTrue(ocrDir.exists())
        assertTrue(ocrDir.isDirectory)
        assertEquals(0, ocrDir.listFiles()?.size ?: -1)
    }

    @Test
    fun `deleteDocwizzArtifacts removes files from both access and primary representations`() {
        val itemId = UUIDv7.randomUUID().toString()
        val folderName = "tekst_$itemId"

        // Create OCR files in access
        val accessOcrDir = baseDir.resolve("$folderName/representations/access/metadata/other/ocr").toFile()
        accessOcrDir.mkdirs()
        val accessFile1 = File(accessOcrDir, "${folderName}_00001.xml").apply { writeText("access-ocr-1") }
        val accessFile2 = File(accessOcrDir, "${folderName}_00002.xml").apply { writeText("access-ocr-2") }

        // Create OCR files in primary
        val primaryOcrDir = baseDir.resolve("$folderName/representations/primary/metadata/other/ocr").toFile()
        primaryOcrDir.mkdirs()
        val primaryFile1 = File(primaryOcrDir, "${folderName}_00001.xml").apply { writeText("primary-ocr-1") }
        val primaryFile2 = File(primaryOcrDir, "${folderName}_00002.xml").apply { writeText("primary-ocr-2") }

        assertTrue(accessFile1.exists() && accessFile2.exists())
        assertTrue(primaryFile1.exists() && primaryFile2.exists())

        reorderFiles.deleteDocwizzArtifacts(itemId, baseDir)

        // Both folders should exist but be empty
        assertTrue(accessOcrDir.exists() && accessOcrDir.isDirectory)
        assertTrue(primaryOcrDir.exists() && primaryOcrDir.isDirectory)
        assertEquals(0, accessOcrDir.listFiles()?.size ?: -1)
        assertEquals(0, primaryOcrDir.listFiles()?.size ?: -1)
    }

    @Test
    fun `deleteDocwizzArtifacts removes files from primary only when access does not exist`() {
        val itemId = UUIDv7.randomUUID().toString()
        val folderName = "tekst_$itemId"

        // Create OCR files only in primary (access folder does not exist)
        val primaryOcrDir = baseDir.resolve("$folderName/representations/primary/metadata/other/ocr").toFile()
        primaryOcrDir.mkdirs()
        val primaryFile1 = File(primaryOcrDir, "${folderName}_00001.xml").apply { writeText("primary-ocr-1") }
        val primaryFile2 = File(primaryOcrDir, "${folderName}_00002.xml").apply { writeText("primary-ocr-2") }

        assertTrue(primaryFile1.exists() && primaryFile2.exists())
        assertFalse(
            baseDir.resolve("$folderName/representations/access/metadata/other/ocr").toFile().exists(),
            "Access OCR directory should not exist"
        )

        reorderFiles.deleteDocwizzArtifacts(itemId, baseDir)

        // Primary folder should exist but be empty
        assertTrue(primaryOcrDir.exists() && primaryOcrDir.isDirectory)
        assertEquals(0, primaryOcrDir.listFiles()?.size ?: -1)
    }

    @Test
    fun `deleteDocwizzArtifacts removes all content types including nested directories`() {
        val itemId = UUIDv7.randomUUID().toString()
        val folderName = "tekst_$itemId"
        val ocrDir = baseDir.resolve("$folderName/representations/access/metadata/other/ocr").toFile()
        ocrDir.mkdirs()

        // Mixed contents: xml, txt, bin and nested directory with files
        val xml = File(ocrDir, "${folderName}_00001.xml").apply { writeText("xml") }
        val txt = File(ocrDir, "readme.txt").apply { writeText("txt") }
        val bin = File(ocrDir, "blob.bin").apply { writeBytes(byteArrayOf(1, 2, 3)) }
        val nested = File(ocrDir, "nested").apply { mkdirs() }
        val nestedFile = File(nested, "nested.json").apply { writeText("{}") }

        assertTrue(xml.exists() && txt.exists() && bin.exists() && nested.exists() && nestedFile.exists())

        reorderFiles.deleteDocwizzArtifacts(itemId, baseDir)

        assertTrue(ocrDir.exists() && ocrDir.isDirectory)
        assertEquals(0, ocrDir.listFiles()?.size ?: -1, "OCR directory should be emptied completely")
    }

    @Test
    fun `deleteDocwizzArtifacts also removes docwizz marker files recursively across item folder`() {
        val itemId = UUIDv7.randomUUID().toString()
        val folderName = "tekst_$itemId"
        val itemDir = baseDir.resolve(folderName).toFile().apply { mkdirs() }

        val rootRdy = File(itemDir, "job.rdy").apply { writeText("ready") }
        val nestedTknDir = File(itemDir, "representations/access/metadata/docwizz").apply { mkdirs() }
        val nestedTkn = File(nestedTknDir, "lock.tkn").apply { writeText("token") }
        val deepWrkDir = File(itemDir, "representations/primary/data/sub/work").apply { mkdirs() }
        val deepWrk = File(deepWrkDir, "process.wrk").apply { writeText("working") }
        val keepFile = File(itemDir, "keep.me").apply { writeText("keep") }

        assertTrue(rootRdy.exists() && nestedTkn.exists() && deepWrk.exists() && keepFile.exists())

        reorderFiles.deleteDocwizzArtifacts(itemId, baseDir)

        assertFalse(rootRdy.exists(), "Root .rdy file should be deleted")
        assertFalse(nestedTkn.exists(), "Nested .tkn file should be deleted")
        assertFalse(deepWrk.exists(), "Deep .wrk file should be deleted")
        assertTrue(keepFile.exists(), "Non-docwizz file should not be deleted")
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
        val mockS3Client = mockk<MinioClient>(relaxed = true)
        every { mockS3Client.copyObject(any()) } throws RuntimeException("Simulated S3 failure")

        mockkObject(S3ClientFactory)
        every { S3ClientFactory.getS3Client(any(), any(), any(), any()) } returns mockS3Client

        val reorderFilesWithUuid = ReorderFiles(uuidProvider = { fixedUuid })

        val inputJson = mapper.writeValueAsString(flowFile)

        val diskFiles = createDiskFiles(changes, baseDir)
        val ocrFiles = createOcrFiles(changes, baseDir, fixedUuid)

        val failingRunner = setupTestRunner(reorderFilesWithUuid)
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

    @Test
    fun `when all images from item A are assigned to item B, item A's folder should be cleaned up on disk and in S3`() {
        val itemA = "019a3aa3-d0af-7658-9a44-aaaaaaaaaaaa"
        val itemB = "019a3aa3-d0af-7658-9a44-bbbbbbbbbbbb"
        val folderA = "tekst_$itemA"
        val folderB = "tekst_$itemB"

        // Item B's orderedImageIds are all of item A's source files,
        // so item A becomes a pure source with no files left after the rename.
        val inputJson = """
            {
              "batchId": "test-batch",
              "changes": [
                {
                  "itemId": "$itemB",
                  "orderedImageIds": [
                    "${folderA}_00001",
                    "${folderA}_00002"
                  ]
                }
              ]
            }
        """.trimIndent()
        val parsedChanges = mapper.readTree(inputJson)["changes"]

        // Seed disk: item A has 2 .tif files in access+primary, plus 2 OCR xmls.
        val diskFiles = createDiskFiles(parsedChanges, baseDir)
        val ocrFiles = createOcrFiles(parsedChanges, baseDir, itemA)
        val s3Keys = createS3Files(parsedChanges, this, testPrefix)

        // Sanity checks before run
        val itemADir = baseDir.resolve(folderA).toFile()
        assertTrue(itemADir.exists(), "Item A folder should exist before run")
        assertTrue(ocrFiles.all { it.exists() }, "OCR files should exist before run")
        assertTrue(diskFiles.all { it.exists() }, "Source .tif files should exist before run")
        assertTrue(s3Keys.all { keyExists(it) }, "Source S3 keys should exist before run")

        runner.enqueue(inputJson)
        runner.run()
        runner.assertAllFlowFilesTransferred(ReorderFiles.REL_SUCCESS, 1)

        // Item A's entire folder must be gone on disk (data dirs + OCR + parent folder)
        assertFalse(itemADir.exists(), "Item A folder should be fully deleted on disk after cleanup")
        ocrFiles.forEach { ocr ->
            assertFalse(ocr.exists(), "OCR file for item A should be deleted: ${ocr.absolutePath}")
        }

        // Item B should contain the renamed files
        listOf(1, 2).forEach { page ->
            val pageStr = "%05d".format(page)
            val accessFile = baseDir.resolve("$folderB/representations/access/data/${folderB}_$pageStr.tif").toFile()
            val primaryFile = baseDir.resolve("$folderB/representations/primary/data/${folderB}_$pageStr.tif").toFile()
            assertTrue(accessFile.exists(), "Renamed access file should exist for B page $page")
            assertTrue(primaryFile.exists(), "Renamed primary file should exist for B page $page")
        }

        // S3: no keys should remain under item A's prefix
        val remainingItemAKeys = listAllKeys().filter { it.startsWith("$testPrefix/$folderA/") }
        assertTrue(
            remainingItemAKeys.isEmpty(),
            "No S3 keys should remain under item A's prefix; found: $remainingItemAKeys"
        )

        // S3: renamed files should exist under item B's prefix
        listOf(1, 2).forEach { page ->
            val pageStr = "%05d".format(page)
            assertTrue(
                keyExists("$testPrefix/$folderB/representations/access/data/${folderB}_$pageStr.tif"),
                "Renamed access key should exist in S3 for B page $page"
            )
            assertTrue(
                keyExists("$testPrefix/$folderB/representations/primary/data/${folderB}_$pageStr.tif"),
                "Renamed primary key should exist in S3 for B page $page"
            )
        }
        assertTrue(
            listAllKeys().none { it.startsWith("tmp_") },
            "No temp keys should remain after successful run"
        )
    }

    @Test
    fun `when moving subset of files from item A to item B, source OCR files are deleted even if source folder remains`() {
        val itemA = "019a3aa3-d0af-7658-9a44-cccccccccccc"
        val itemB = "019a3aa3-d0af-7658-9a44-dddddddddddd"
        val folderA = "tekst_$itemA"
        val folderB = "tekst_$itemB"

        val movedSourceName = "${folderA}_00001.tif"
        val remainingSourceName = "${folderA}_00002.tif"

        val inputJson = """
            {
              "batchId": "test-batch-partial-move",
              "changes": [
                {
                  "itemId": "$itemB",
                  "orderedImageIds": [
                    "${folderA}_00001"
                  ]
                }
              ]
            }
        """.trimIndent()

        // Seed disk for moved file
        TestFileUtils.createFile(baseDir, "access", movedSourceName)
        TestFileUtils.createFile(baseDir, "primary", movedSourceName)

        // Keep one file in item A so source folder is not fully emptied
        val remainingAccess = TestFileUtils.createFile(baseDir, "access", remainingSourceName)
        val remainingPrimary = TestFileUtils.createFile(baseDir, "primary", remainingSourceName)

        // Seed OCR in item A for both representations
        val accessOcrDir = baseDir.resolve("$folderA/representations/access/metadata/other/ocr").toFile().apply { mkdirs() }
        val primaryOcrDir = baseDir.resolve("$folderA/representations/primary/metadata/other/ocr").toFile().apply { mkdirs() }
        val accessOcr = File(accessOcrDir, "${folderA}_00001.xml").apply { writeText("access-ocr") }
        val primaryOcr = File(primaryOcrDir, "${folderA}_00001.xml").apply { writeText("primary-ocr") }

        // Seed S3 for moved file
        putObject("$testPrefix/$folderA/representations/access/data/$movedSourceName")
        putObject("$testPrefix/$folderA/representations/primary/data/$movedSourceName")

        runner.enqueue(inputJson)
        runner.run()
        runner.assertAllFlowFilesTransferred(ReorderFiles.REL_SUCCESS, 1)

        // Source folder should remain because one data file is still present
        assertTrue(baseDir.resolve(folderA).toFile().exists(), "Item A folder should remain after partial move")
        assertTrue(remainingAccess.exists(), "Unmoved source access file should remain")
        assertTrue(remainingPrimary.exists(), "Unmoved source primary file should remain")

        // Source OCR must still be deleted
        assertFalse(accessOcr.exists(), "Source access OCR should be deleted")
        assertFalse(primaryOcr.exists(), "Source primary OCR should be deleted")

        // Moved file should now exist under item B
        assertTrue(
            baseDir.resolve("$folderB/representations/access/data/${folderB}_00001.tif").toFile().exists(),
            "Renamed access file should exist under target item"
        )
        assertTrue(
            baseDir.resolve("$folderB/representations/primary/data/${folderB}_00001.tif").toFile().exists(),
            "Renamed primary file should exist under target item"
        )
    }

    @Test
    fun `cross-item move with empty prefix deletes source S3 keys and creates destination keys without leading slash`() {
        val itemA = "019a3aa3-d0af-7658-9a44-eeeeeeeeeeee"
        val itemB = "019a3aa3-d0af-7658-9a44-ffffffffffff"
        val folderA = "tekst_$itemA"
        val folderB = "tekst_$itemB"

        val emptyPrefixRunner = setupTestRunner(ReorderFiles()).apply {
            setProperty(ReorderFiles.PREFIX, "")
            assertValid()
        }

        val inputJson = """
            {
              "batchId": "test-empty-prefix",
              "changes": [
                {
                  "itemId": "$itemB",
                  "orderedImageIds": [
                    "${folderA}_00001",
                    "${folderA}_00002"
                  ]
                }
              ]
            }
        """.trimIndent()

        // Seed disk: item A has 2 .tif files in access + primary
        TestFileUtils.createFile(baseDir, "access", "${folderA}_00001.tif")
        TestFileUtils.createFile(baseDir, "access", "${folderA}_00002.tif")
        TestFileUtils.createFile(baseDir, "primary", "${folderA}_00001.tif")
        TestFileUtils.createFile(baseDir, "primary", "${folderA}_00002.tif")

        // Seed S3 WITHOUT any prefix (top-level keys)
        val sourceKeys = listOf(
            "$folderA/representations/access/data/${folderA}_00001.tif",
            "$folderA/representations/access/data/${folderA}_00002.tif",
            "$folderA/representations/primary/data/${folderA}_00001.tif",
            "$folderA/representations/primary/data/${folderA}_00002.tif"
        )
        sourceKeys.forEach { putObject(it) }
        assertTrue(sourceKeys.all { keyExists(it) }, "Source S3 keys should exist before run")

        emptyPrefixRunner.enqueue(inputJson)
        emptyPrefixRunner.run()
        emptyPrefixRunner.assertAllFlowFilesTransferred(ReorderFiles.REL_SUCCESS, 1)

        // S3: source keys must be gone
        val remainingSourceKeys = listAllKeys().filter { it.startsWith("$folderA/") }
        assertTrue(
            remainingSourceKeys.isEmpty(),
            "All source S3 keys should be deleted after cross-item move with empty prefix; found: $remainingSourceKeys"
        )

        // S3: destination keys must exist without a leading slash
        listOf(1, 2).forEach { page ->
            val pageStr = "%05d".format(page)
            assertTrue(
                keyExists("$folderB/representations/access/data/${folderB}_$pageStr.tif"),
                "Destination access S3 key (no prefix) should exist for page $page"
            )
            assertTrue(
                keyExists("$folderB/representations/primary/data/${folderB}_$pageStr.tif"),
                "Destination primary S3 key (no prefix) should exist for page $page"
            )
        }

        assertTrue(
            listAllKeys().none { it.startsWith("tmp_") },
            "No temp keys should remain after successful run"
        )
    }

    @Test
    fun `addInstruction preserves jp2 file extension when provided`() {
        val itemId = UUIDv7.randomUUID().toString()
        val folderName = "tekst_$itemId"
        val jp2Images = mapper.readValue("""["${folderName}_00001.jp2"]""", JsonNode::class.java)

        val instructions = reorderFiles.addInstruction(itemId, jp2Images, "%05d", baseDir)

        assertEquals(1, instructions.size)
        assertEquals("${folderName}_00001.jp2", instructions[0].originalName)
        assertEquals("${folderName}_00001.jp2", instructions[0].newName)
    }

    @Test
    fun `addInstruction discovers jp2 extension from disk when no extension given`() {
        val itemId = UUIDv7.randomUUID().toString()
        val folderName = "tekst_$itemId"
        val originalFilename = "${folderName}_00001"

        // Create only a .jp2 file on disk — no .tif exists
        TestFileUtils.createFile(baseDir, "access", "$originalFilename.jp2")

        val images = mapper.readValue("""["$originalFilename"]""", JsonNode::class.java)
        val instructions = reorderFiles.addInstruction(itemId, images, "%05d", baseDir)

        assertEquals(1, instructions.size)
        assertEquals("$originalFilename.jp2", instructions[0].originalName,
            "Should discover .jp2 from disk instead of defaulting to .tif")
        assertEquals("${folderName}_00001.jp2", instructions[0].newName,
            "New name should preserve the discovered .jp2 extension")
    }

    @Test
    fun `addInstruction discovers extension from source item folder during cross-item move`() {
        val sourceItemId = "019eab0f-8ca5-7936-b4ea-0a0aeeb51b6c"
        val targetItemId = "019eab0f-8ca5-7936-b4ea-0a09af2e3da4"
        val sourceFolderName = "tekst_$sourceItemId"

        // Source file is a .jp2 in the SOURCE folder — no .tif exists anywhere
        TestFileUtils.createFile(baseDir, "access", "${sourceFolderName}_00001.jp2")

        val images = mapper.readValue("""["${sourceFolderName}_00001"]""", JsonNode::class.java)
        val instructions = reorderFiles.addInstruction(targetItemId, images, "%05d", baseDir)

        assertEquals(1, instructions.size)
        assertEquals("${sourceFolderName}_00001.jp2", instructions[0].originalName,
            "Should find .jp2 in the source item's folder, not the target folder")
        assertEquals("tekst_${targetItemId}_00001.jp2", instructions[0].newName,
            "New name should use target itemId with discovered .jp2 extension")
    }

    @Test
    fun `addInstruction creates one instruction per extension when both tif and jp2 exist and no extension given`() {
        val itemId = UUIDv7.randomUUID().toString()
        val folderName = "tekst_$itemId"
        val originalFilename = "${folderName}_00001"

        // Create both .tif and .jp2 on disk
        TestFileUtils.createFile(baseDir, "access", "$originalFilename.tif")
        TestFileUtils.createFile(baseDir, "access", "$originalFilename.jp2")

        val images = mapper.readValue("""["$originalFilename"]""", JsonNode::class.java)
        val instructions = reorderFiles.addInstruction(itemId, images, "%05d", baseDir)

        assertEquals(2, instructions.size, "Should produce one instruction per discovered extension")
        val originals = instructions.map { it.originalName }.toSet()
        val newNames = instructions.map { it.newName }.toSet()
        assertTrue(originals.contains("$originalFilename.tif"), "Should include .tif instruction")
        assertTrue(originals.contains("$originalFilename.jp2"), "Should include .jp2 instruction")
        assertTrue(newNames.contains("${folderName}_00001.tif"))
        assertTrue(newNames.contains("${folderName}_00001.jp2"))
    }

    @Test
    fun `processor handles mixed tif and jp2 files correctly`() {
        val itemId = UUIDv7.randomUUID().toString()
        val folderName = "tekst_$itemId"

        // Create mix of .tif and .jp2 files in both representations on disk
        TestFileUtils.createFile(baseDir, "access", "${folderName}_00001.tif")
        TestFileUtils.createFile(baseDir, "access", "${folderName}_00002.jp2")
        TestFileUtils.createFile(baseDir, "primary", "${folderName}_00001.tif")
        TestFileUtils.createFile(baseDir, "primary", "${folderName}_00002.jp2")

        // S3 always stores .tif keys regardless of the on-disk format
        putObject("$testPrefix/$folderName/representations/access/data/${folderName}_00001.tif")
        putObject("$testPrefix/$folderName/representations/access/data/${folderName}_00002.tif")
        putObject("$testPrefix/$folderName/representations/primary/data/${folderName}_00001.tif")
        putObject("$testPrefix/$folderName/representations/primary/data/${folderName}_00002.tif")

        // Reorder: swap positions (jp2 moves to position 1, tif moves to position 2)
        val inputJson = """
            {
              "batchId": "test-mixed-ext",
              "changes": [
                {
                  "itemId": "$itemId",
                  "orderedImageIds": [
                    "${folderName}_00002.jp2",
                    "${folderName}_00001.tif"
                  ]
                }
              ]
            }
        """.trimIndent()

        runner.enqueue(inputJson)
        runner.run()
        runner.assertAllFlowFilesTransferred(ReorderFiles.REL_SUCCESS, 1)

        // Disk: extensions must be preserved in new positions
        assertTrue(baseDir.resolve("$folderName/representations/access/data/${folderName}_00001.jp2").toFile().exists(),
            "Renamed access .jp2 should exist at position 1")
        assertTrue(baseDir.resolve("$folderName/representations/access/data/${folderName}_00002.tif").toFile().exists(),
            "Renamed access .tif should exist at position 2")
        assertTrue(baseDir.resolve("$folderName/representations/primary/data/${folderName}_00001.jp2").toFile().exists(),
            "Renamed primary .jp2 should exist at position 1")
        assertTrue(baseDir.resolve("$folderName/representations/primary/data/${folderName}_00002.tif").toFile().exists(),
            "Renamed primary .tif should exist at position 2")

        // S3: keys are always .tif, moved to new positions
        assertTrue(keyExists("$testPrefix/$folderName/representations/access/data/${folderName}_00001.tif"),
            "S3 access key at new position 1 should be .tif")
        assertTrue(keyExists("$testPrefix/$folderName/representations/access/data/${folderName}_00002.tif"),
            "S3 access key at new position 2 should be .tif")
        assertTrue(keyExists("$testPrefix/$folderName/representations/primary/data/${folderName}_00001.tif"),
            "S3 primary key at new position 1 should be .tif")
        assertTrue(keyExists("$testPrefix/$folderName/representations/primary/data/${folderName}_00002.tif"),
            "S3 primary key at new position 2 should be .tif")
    }
}
