package no.nb.nifi.tekst.processors

import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.minio.UploadObjectArgs
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.nio.file.Path


class DownloadMultipleS3FilesByPrefixTest {

    companion object {
        private lateinit var minioContainer: GenericContainer<*>
        private lateinit var minioClient: MinioClient
        private lateinit var minioServerUrl: String

        val ACCESS_KEY = "minioadmin"
        val SECRET_KEY = "minioAdmin123"
        val BUCKET = "test-bucket"
        val REGION = "NBR"

        @BeforeAll
        @JvmStatic
        fun setup() {
            minioContainer = GenericContainer(DockerImageName.parse("minio/minio"))
                .withExposedPorts(9000)
                .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                .withEnv("MINIO_REGION", REGION)
                .withEnv("MINIO_REGION_NAME", REGION)
                .withCommand("server /data")

            minioContainer.start()

            val endpoint = "http://${minioContainer.host}:${minioContainer.getFirstMappedPort()}"
            minioServerUrl = endpoint
            minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(ACCESS_KEY, SECRET_KEY)
                .region(REGION)
                .build()

            // Create a bucket and upload a test file
            val makeBucketArgs = MakeBucketArgs.builder()
                .bucket(BUCKET)
                .build()
            minioClient.makeBucket(makeBucketArgs)

            minioClient.uploadObject(
                UploadObjectArgs.builder()
                    .bucket(BUCKET)
                    .`object`("NEWSPAPER/143aef46-9abd-11ef-a03f-fddd5c381f23/143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png")
                    .filename("src/test/resources/S3-data/logos/NB-logo-no-hvit.png")
                    .build()
            )
            minioClient.uploadObject(
                UploadObjectArgs.builder()
                    .bucket(BUCKET)
                    .`object`("NEWSPAPER/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23/representations/primary/data/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png")
                    .filename("src/test/resources/S3-data/logos/NB-logo-no-hvit.png")
                    .build()
            )
            minioClient.uploadObject(
                UploadObjectArgs.builder()
                    .bucket(BUCKET)
                    .`object`("NEWSPAPER/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23/representations/access/data/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png")
                    .filename("src/test/resources/S3-data/logos/NB-logo-no-hvit.png")
                    .build()
            )

        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            minioContainer.stop()
        }
    }

    @Test
    fun testAddingRequiredProperties() {
        val runner = TestRunners.newTestRunner(DownloadMultipleS3FilesByPrefix::class.java)

        runner.setProperty(DownloadMultipleS3FilesByPrefix.BUCKET, BUCKET)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ACCESS_KEY, ACCESS_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.SECRET_KEY, SECRET_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.REGION, REGION)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "test-folder")
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ENDPOINT, minioServerUrl)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, "folder2")
    }

    @Test
    fun listFilesAndDownloadFilesAccessAndPrimary(@TempDir tempDir: Path) {
        val runner = TestRunners.newTestRunner(DownloadMultipleS3FilesByPrefix::class.java)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.BUCKET, BUCKET)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ACCESS_KEY, ACCESS_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.SECRET_KEY, SECRET_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.REGION, REGION)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "NEWSPAPER/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23")
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ENDPOINT, minioServerUrl)

        val downloadDir = tempDir.toAbsolutePath().toString()
        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, downloadDir)

        runner.enqueue("Hello world")

        runner.run()

        // check that the files are downloaded - find by name rather than relying on listFiles() order
        val downloadedFilesDir = tempDir.toFile()
        val representationsDir = File(downloadedFilesDir, "representations")
        val accessDir = File(representationsDir, "access/data")
        val primaryDir = File(representationsDir, "primary/data")
        val accessFile = File(accessDir, "tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png")
        val primaryFile = File(primaryDir, "tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png")

        assertTrue(accessFile.exists()) { "Access file should exist at ${accessFile.path}" }
        assertTrue(accessFile.path.endsWith("representations/access/data/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png"))
        assertTrue(primaryFile.exists()) { "Primary file should exist at ${primaryFile.path}" }
        assertTrue(primaryFile.path.endsWith("representations/primary/data/tekst_143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png"))
        runner.assertAllFlowFilesTransferred("success")
        // cleanup handled automatically by @TempDir
    }

    @Test
    fun listFilesAndDownloadFiles(@TempDir tempDir: Path) {
        val runner = TestRunners.newTestRunner(DownloadMultipleS3FilesByPrefix::class.java)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.BUCKET, BUCKET)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ACCESS_KEY, ACCESS_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.SECRET_KEY, SECRET_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.REGION, REGION)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "NEWSPAPER/143aef46-9abd-11ef-a03f-fddd5c381f23")
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ENDPOINT, minioServerUrl)

        val downloadDir = tempDir.toAbsolutePath().toString()
        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, downloadDir)

        runner.enqueue("Hello world")

        runner.run()

        // check that the files are downloaded
        val downloadedFile = File(tempDir.toFile(), "143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png")
        assertTrue(downloadedFile.exists()) { "Downloaded file should exist at ${downloadedFile.path}" }
        assertEquals("143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png", downloadedFile.name)

        runner.assertAllFlowFilesTransferred("success")
        // cleanup handled automatically by @TempDir
    }

    @Test
    fun shouldFailWhenObjectDoesNotExist(@TempDir tempDir: Path) {
        val runner = TestRunners.newTestRunner(DownloadMultipleS3FilesByPrefix::class.java)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.BUCKET, BUCKET)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ACCESS_KEY, ACCESS_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.SECRET_KEY, SECRET_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.REGION, REGION)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "NEWSPAPER/abc")
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ENDPOINT, minioServerUrl)

        val downloadDir = tempDir.toAbsolutePath().toString()
        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, downloadDir)

        runner.enqueue("Hello world")

        runner.run()
        
        runner.assertAllFlowFilesTransferred("failure")
        // cleanup handled automatically by @TempDir
    }

}
