package no.nb.nifi.tekst.processors

import io.mockk.every
import io.mockk.mockkObject
import io.mockk.unmockkObject
import no.nb.nifi.tekst.util.S3ClientFactory
import no.nb.utils.MinIOTestBase
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DeleteMinioFileTest : MinIOTestBase() {

    private lateinit var runner: TestRunner

    private val itemId = "019a3aa3-d0af-7658-9a44-5df904c51bec"
    private val folderName = "tekst_$itemId"
    private val testPrefix = "NEWSPAPER"

    @BeforeEach
    fun setUp() {
        runner = setupTestRunner(includePrefix = true)
    }

    @AfterEach
    fun tearDown() {
        unmockkObject(S3ClientFactory)
    }

    private fun setupTestRunner(includePrefix: Boolean): TestRunner =
        TestRunners.newTestRunner(DeleteMinioFile()).apply {
            setProperty(DeleteMinioFile.ITEM_ID, itemId)
            setProperty(DeleteMinioFile.ENDPOINT, s3Endpoint)
            setProperty(DeleteMinioFile.BUCKET, BUCKET)
            setProperty(DeleteMinioFile.ACCESS_KEY, s3AccessKey)
            setProperty(DeleteMinioFile.SECRET_KEY, s3SecretKey)
            setProperty(DeleteMinioFile.REGION, REGION)
            if (includePrefix) setProperty(DeleteMinioFile.PREFIX, testPrefix)
            assertValid()
        }

    @Test
    fun `deletes all keys under tekst_itemId when prefix is provided`() {
        // Keys that should be deleted (under the configured prefix + tekst_<itemId>/)
        val targetKeys = listOf(
            "$testPrefix/$folderName/representations/access/data/${folderName}_00001.tif",
            "$testPrefix/$folderName/representations/primary/data/${folderName}_00001.tif",
            "$testPrefix/$folderName/representations/access/metadata/other/ocr/${folderName}_00001.xml"
        )
        // Keys that must NOT be deleted: different itemId, and same itemId but outside the prefix
        val unrelatedKeys = listOf(
            "$testPrefix/tekst_other-item/representations/access/data/tekst_other-item_00001.tif",
            "$folderName/representations/access/data/${folderName}_00001.tif"
        )
        (targetKeys + unrelatedKeys).forEach { putObject(it) }

        runner.enqueue("".toByteArray())
        runner.run()

        runner.assertAllFlowFilesTransferred(DeleteMinioFile.REL_SUCCESS, 1)
        targetKeys.forEach {
            assertFalse(keyExists(it), "Key should have been deleted: $it")
        }
        unrelatedKeys.forEach {
            assertTrue(keyExists(it), "Unrelated key should not have been deleted: $it")
        }
    }

    @Test
    fun `deletes all keys under tekst_itemId when no prefix is provided`() {
        runner = setupTestRunner(includePrefix = false)

        val targetKeys = listOf(
            "$folderName/representations/access/data/${folderName}_00001.tif",
            "$folderName/representations/primary/data/${folderName}_00001.tif"
        )
        val unrelatedKey = "tekst_other-item/representations/access/data/tekst_other-item_00001.tif"
        (targetKeys + unrelatedKey).forEach { putObject(it) }

        runner.enqueue("".toByteArray())
        runner.run()

        runner.assertAllFlowFilesTransferred(DeleteMinioFile.REL_SUCCESS, 1)
        targetKeys.forEach {
            assertFalse(keyExists(it), "Key should have been deleted: $it")
        }
        assertTrue(keyExists(unrelatedKey), "Unrelated key should not have been deleted: $unrelatedKey")
    }

    @Test
    fun `routes flowfile to failure when S3 client throws`() {
        // Inject the failure at the client-factory level so the test does not depend on
        // which MinIO call (listObjects / statObject / removeObjects / ...) the
        // deletion implementation happens to invoke first.
        mockkObject(S3ClientFactory)
        every {
            S3ClientFactory.getS3Client(any(), any(), any(), any())
        } throws RuntimeException("Simulated S3 failure")

        runner.enqueue("".toByteArray())
        runner.run()

        runner.assertAllFlowFilesTransferred(DeleteMinioFile.REL_FAILURE, 1)
    }
}

