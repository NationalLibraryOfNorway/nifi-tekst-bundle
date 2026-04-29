package no.nb.nifi.tekst.processors

import no.nb.utils.MinIOTestBase
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path

class DownloadMultipleS3FilesByPrefixTest : MinIOTestBase() {

    @BeforeEach
    fun uploadTestFiles() {
        uploadTestObject(
            key = "NEWSPAPER/143aef46-9abd-11ef-a03f-fddd5c381f23/143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png",
            filePath = "src/test/resources/S3-data/logos/NB-logo-no-hvit.png"
        )
        uploadTestObject(
            key = "NEWSPAPER/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23/representations/primary/data/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png",
            filePath = "src/test/resources/S3-data/logos/NB-logo-no-hvit.png"
        )
        uploadTestObject(
            key = "NEWSPAPER/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23/representations/access/data/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png",
            filePath = "src/test/resources/S3-data/logos/NB-logo-no-hvit.png"
        )
    }

    private fun setupRunner(): TestRunner =
        TestRunners.newTestRunner(DownloadMultipleS3FilesByPrefix::class.java).apply {
            setProperty(DownloadMultipleS3FilesByPrefix.BUCKET, BUCKET)
            setProperty(DownloadMultipleS3FilesByPrefix.ACCESS_KEY, s3AccessKey)
            setProperty(DownloadMultipleS3FilesByPrefix.SECRET_KEY, s3SecretKey)
            setProperty(DownloadMultipleS3FilesByPrefix.ENDPOINT, s3Endpoint)
            setProperty(DownloadMultipleS3FilesByPrefix.REGION, REGION) // now using MinIOTestBase.REGION
        }

    @Test
    fun `processor accepts valid properties`() {
        val runner = setupRunner()
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "test-folder")
        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, "folder2")
        runner.assertValid()
    }

    @Test
    fun `should download access and primary files to correct directory structure`(@TempDir tempDir: Path) {
        val runner = setupRunner()
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "NEWSPAPER/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23")
        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, tempDir.toAbsolutePath().toString())
        runner.enqueue("Hello world")
        runner.run()

        val representationsDir = File(tempDir.toFile(), "representations")
        val accessFile = File(representationsDir, "access/data/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png")
        val primaryFile = File(representationsDir, "primary/data/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png")

        assertTrue(accessFile.exists()) { "Access file should exist at ${accessFile.path}" }
        assertTrue(accessFile.path.endsWith("representations/access/data/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png"))
        assertTrue(primaryFile.exists()) { "Primary file should exist at ${primaryFile.path}" }
        assertTrue(primaryFile.path.endsWith("representations/primary/data/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png"))
        runner.assertAllFlowFilesTransferred("success")
    }

    @Test
    fun `should download file to local folder`(@TempDir tempDir: Path) {
        val runner = setupRunner()
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "NEWSPAPER/143aef46-9abd-11ef-a03f-fddd5c381f23")
        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, tempDir.toAbsolutePath().toString())
        runner.enqueue("Hello world")
        runner.run()

        val downloadedFile = File(tempDir.toFile(), "143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png")
        assertTrue(downloadedFile.exists()) { "Downloaded file should exist at ${downloadedFile.path}" }
        assertEquals("143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png", downloadedFile.name)
        runner.assertAllFlowFilesTransferred("success")
    }

    @Test
    fun `should route to failure when no objects found for prefix`(@TempDir tempDir: Path) {
        val runner = setupRunner()
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "NEWSPAPER/abc")
        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, tempDir.toAbsolutePath().toString())
        runner.enqueue("Hello world")
        runner.run()

        runner.assertAllFlowFilesTransferred("failure")
    }
}