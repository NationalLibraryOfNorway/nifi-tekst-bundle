package no.nb.nifi.tekst.processors

import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.minio.UploadObjectArgs
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.nio.file.Paths


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
            println(minioContainer.getFirstMappedPort())

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
    fun listFilesAndDownloadFiles() {
        val runner = TestRunners.newTestRunner(DownloadMultipleS3FilesByPrefix::class.java)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.BUCKET, BUCKET)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ACCESS_KEY, ACCESS_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.SECRET_KEY, SECRET_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.REGION, REGION)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "NEWSPAPER/143aef46-9abd-11ef-a03f-fddd5c381f23")
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ENDPOINT, minioServerUrl)

        val projectFolder = Paths.get("").toAbsolutePath().toString()
        val downloadedFilesFolder = "src/test/resources/downloaded-files"

        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, "$projectFolder/$downloadedFilesFolder")

        runner.enqueue("Hello world")

        runner.run()

        // check that the files are downloaded
        val downloadedFiles = File(downloadedFilesFolder).listFiles()
        assert(downloadedFiles?.size == 1)
        assert(downloadedFiles?.get(0)?.name == "143aef46-9abd-11ef-a03f-fddd5c381f23_0001.png")

        runner.assertAllFlowFilesTransferred("success")

        // clean up
        downloadedFiles?.forEach { it.delete() }
        File(downloadedFilesFolder).delete()
    }

    @Test
    fun shouldFailWhenObjectDoesNotExist() {
        val runner = TestRunners.newTestRunner(DownloadMultipleS3FilesByPrefix::class.java)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.BUCKET, BUCKET)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ACCESS_KEY, ACCESS_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.SECRET_KEY, SECRET_KEY)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.REGION, REGION)
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "NEWSPAPER/abc")
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ENDPOINT, minioServerUrl)

        val projectFolder = Paths.get("").toAbsolutePath().toString()
        val downloadedFilesFolder = "src/test/resources/downloaded-files"

        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, "$projectFolder/$downloadedFilesFolder")

        runner.enqueue("Hello world")

        runner.run()

        // check that the files are downloaded
        runner.assertAllFlowFilesTransferred("failure")

        // clean up
        File(downloadedFilesFolder).delete()
    }

}
