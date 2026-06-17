package no.nb.nifi.tekst.util

import io.minio.MinioClient
import org.slf4j.LoggerFactory

object S3ClientFactory {
    private val logger = LoggerFactory.getLogger(S3ClientFactory::class.java)

    /**
     * Builds a S3 client from processor properties.
     */
    fun getS3Client(accessKey: String, secretKey: String, region: String, url: String): MinioClient {
        logger.debug("S3ClientFactory: Creating client — endpoint='{}', region='{}'", url, region)
        return MinioClient
            .builder()
            .endpoint(url)
            .region(region)
            .credentials(accessKey, secretKey)
            .build()
    }
}
