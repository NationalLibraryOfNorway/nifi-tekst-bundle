package no.nb.utils

import io.minio.ListObjectsArgs
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.minio.PutObjectArgs
import io.minio.RemoveObjectsArgs
import io.minio.StatObjectArgs
import io.minio.UploadObjectArgs
import io.minio.errors.ErrorResponseException
import io.minio.messages.DeleteObject
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.io.ByteArrayInputStream
import java.time.Duration

@Testcontainers
abstract class MinIOTestBase {

    companion object {
        const val MINIO_USER = "minioadmin"
        const val MINIO_PASSWORD = "minioadmin"
        const val BUCKET = "test-bucket"
        const val REGION = "NBR"

        @JvmField
        @Container
        val minioContainer: GenericContainer<*> = GenericContainer(DockerImageName.parse("minio/minio:RELEASE.2025-04-22T22-12-26Z"))
            .withExposedPorts(9000)
            .withEnv("MINIO_ROOT_USER", MINIO_USER)
            .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
            .withEnv("MINIO_REGION", REGION)
            .withEnv("MINIO_REGION_NAME", REGION)
            .withCommand("server /data")
            .waitingFor(
                // Wait until MinIO is actually ready to accept requests
                HttpWaitStrategy()
                    .forPath("/minio/health/live")
                    .forPort(9000)
                    .withStartupTimeout(Duration.ofSeconds(30))
            )
    }

    lateinit var minioClient: MinioClient
    val s3Endpoint get() = "http://${minioContainer.host}:${minioContainer.getFirstMappedPort()}"
    val s3AccessKey get() = MINIO_USER
    val s3SecretKey get() = MINIO_PASSWORD

    @BeforeEach
    fun setUpS3() {
        minioClient = MinioClient.builder()
            .endpoint(s3Endpoint)
            .credentials(MINIO_USER, MINIO_PASSWORD)
            .region(REGION)
            .build()

        try {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build())
        } catch (e: ErrorResponseException) {
            if (e.errorResponse().code() != "BucketAlreadyOwnedByYou") throw e
            // Bucket already exists — this is fine
        }
    }

    @AfterEach
    fun tearDownS3() {
        val objects = listAllKeys().map { DeleteObject(it) }
        if (objects.isNotEmpty()) {
            val errors = minioClient.removeObjects(
                RemoveObjectsArgs.builder()
                    .bucket(BUCKET)
                    .objects(objects)
                    .build()
            ).mapNotNull { result ->
                runCatching { result.get() }.getOrNull()  // DeleteError if deletion failed
            }
            if (errors.isNotEmpty()) {
                throw RuntimeException(
                    "Failed to remove ${errors.size} object(s) during teardown: " +
                            errors.joinToString { "${it.objectName()} — ${it.message()}" }
                )
            }
        }
    }

    /**
     * Uploads a file from disk to the bucket.
     * Use this when you need real file content, e.g. images.
     */
    fun uploadTestObject(key: String, filePath: String) {
        minioClient.uploadObject(
            UploadObjectArgs.builder()
                .bucket(BUCKET)
                .`object`(key)
                .filename(filePath)
                .build()
        )
    }

    /**
     * Puts a simple string content object into the bucket.
     * Use this when file content doesn't matter, e.g. for rename tests.
     */
    fun putObject(key: String, content: String = "dummy content") {
        val bytes = content.toByteArray()
        minioClient.putObject(
            PutObjectArgs.builder()
                .bucket(BUCKET)
                .`object`(key)
                .stream(ByteArrayInputStream(bytes), bytes.size.toLong(), -1)
                .contentType("application/octet-stream")
                .build()
        )
    }

    fun keyExists(key: String): Boolean =
        try {
            minioClient.statObject(
                StatObjectArgs.builder().bucket(BUCKET).`object`(key).build()
            )
            true
        } catch (e: ErrorResponseException) {
            if (e.errorResponse().code() == "NoSuchKey") false else throw e
        }

    fun listAllKeys(): List<String> =
        minioClient.listObjects(
            ListObjectsArgs.builder().bucket(BUCKET).recursive(true).build()
        ).map { it.get().objectName() }
}