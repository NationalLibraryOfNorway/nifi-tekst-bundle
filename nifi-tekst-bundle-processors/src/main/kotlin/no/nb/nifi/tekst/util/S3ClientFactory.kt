package no.nb.nifi.tekst.util

import io.minio.MinioClient
import org.slf4j.LoggerFactory

object S3ClientFactory {
    private val logger = LoggerFactory.getLogger(S3ClientFactory::class.java)

    /**
     * Builds a MinIO client from processor properties.
     */
    fun getS3Client(accessKey: String, secretKey: String, region: String, url: String): MinioClient {
        logger.debug("MinioClientFactory: Creating client — endpoint='{}', region='{}'", url, region)
        return MinioClient
            .builder()
            .endpoint(url)
            .region(region)
            .credentials(accessKey, secretKey)
            .build()
    }
}
