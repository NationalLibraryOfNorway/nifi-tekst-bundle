package no.nb.utils

import io.minio.CopyObjectArgs
import io.minio.CopySource
import io.minio.ListObjectsArgs
import io.minio.MinioClient
import io.minio.RemoveObjectArgs
import io.minio.RemoveObjectsArgs
import io.minio.StatObjectArgs
import io.minio.errors.ErrorResponseException
import io.minio.messages.DeleteObject
import org.slf4j.LoggerFactory

/**
 * Generic S3/MinIO helpers shared by processors and other utility classes.
 */
object S3Utils {

    private val logger = LoggerFactory.getLogger(S3Utils::class.java)

    private const val DELETE_BATCH_SIZE = 1000

    /**
     * Returns true if the given key exists in the bucket, false if not found.
     * Any other S3 error (permissions, network) is rethrown.
     */
    fun keyExists(client: MinioClient, bucket: String, key: String): Boolean =
        try {
            client.statObject(StatObjectArgs.builder().bucket(bucket).`object`(key).build())
            true
        } catch (e: ErrorResponseException) {
            if (e.errorResponse().code() == "NoSuchKey") false else throw e
        }

    /**
     * Copies a single object within the same bucket.
     */
    fun copyObjectWithinBucket(client: MinioClient, bucket: String, sourceKey: String, destKey: String) {
        client.copyObject(
            CopyObjectArgs.builder()
                .bucket(bucket)
                .`object`(destKey)
                .source(
                    CopySource.builder()
                        .bucket(bucket)
                        .`object`(sourceKey)
                        .build()
                )
                .build()
        )
    }

    /**
     * Deletes a list of keys one by one, logging each deletion.
     * Used for rollback where we want to attempt all deletions even if one fails.
     */
    fun rollbackDelete(client: MinioClient, bucket: String, keys: List<String>) {
        keys.forEach { key ->
            try {
                client.removeObject(
                    RemoveObjectArgs.builder().bucket(bucket).`object`(key).build()
                )
                logger.warn("Deleted key '$key' from bucket '$bucket'")
            } catch (e: Exception) {
                logger.error("Failed to delete '$key' from bucket '$bucket' (continuing)", e)
            }
        }
    }

    /**
     * Deletes a list of keys in batches of 1000 (S3 API limit per request).
     * Used for bulk cleanup of original and temp keys after a successful rename.
     */
    fun batchDelete(client: MinioClient, bucket: String, keys: List<String>) {
        if (keys.isEmpty()) {
            logger.debug("No keys to delete in bucket '$bucket'")
            return
        }

        val allErrors = mutableListOf<String>()

        keys.chunked(DELETE_BATCH_SIZE).forEach { chunk ->
            val deleteObjects = chunk.map { DeleteObject(it) }
            val results = client.removeObjects(
                RemoveObjectsArgs.builder()
                    .bucket(bucket)
                    .objects(deleteObjects)
                    .build()
            )
            results.forEach { result ->
                val error = result.get()
                val msg = "'${error.objectName()}': ${error.message()}"
                logger.error("Failed to delete key $msg from bucket '$bucket'")
                allErrors.add(msg)
            }
        }

        if (allErrors.isNotEmpty()) {
            throw RuntimeException(
                "Failed to delete ${allErrors.size} key(s) from bucket '$bucket': " +
                        allErrors.joinToString()
            )
        }

        logger.debug("Deleted ${keys.size} keys from bucket '$bucket'")
    }

    /**
     * Deletes all keys in the bucket that share a common prefix.
     * Useful for wiping an itemId's entire folder.
     */
    fun deleteAllKeysWithPrefix(client: MinioClient, bucket: String, keyPrefix: String) {
        val keys = client.listObjects(
            ListObjectsArgs.builder().bucket(bucket).prefix(keyPrefix).recursive(true).build()
        ).map { it.get().objectName() }

        if (keys.isEmpty()) {
            logger.debug("No keys found with prefix '$keyPrefix' in bucket '$bucket'")
            return
        }

        logger.info("Deleting ${keys.size} keys with prefix '$keyPrefix' from bucket '$bucket'")
        batchDelete(client, bucket, keys)
    }
}

