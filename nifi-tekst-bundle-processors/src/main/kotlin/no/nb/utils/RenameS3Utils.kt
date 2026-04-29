package no.nb.utils

import io.minio.CopyObjectArgs
import io.minio.CopySource
import io.minio.MinioClient
import io.minio.RemoveObjectArgs
import io.minio.RemoveObjectsArgs
import io.minio.StatObjectArgs
import io.minio.errors.ErrorResponseException
import io.minio.messages.DeleteObject
import no.nb.models.RenameInstruction
import no.nb.utils.RenameUtils.extractIdFromFilename
import org.slf4j.LoggerFactory

object RenameS3Utils {

    private val logger = LoggerFactory.getLogger(RenameS3Utils::class.java)
    private val KEY_REPRESENTATIONS = listOf(
        "representations/access/data",
        "representations/primary/data"
    )

    /**
     * Renames S3 objects to match a list of rename instructions, using a two-phase
     * staged approach to safely handle swap/cycle conflicts between filenames.
     *
     * Stage:   Copy all source keys to unique temp keys.
     *          Nothing is written to final destinations yet, so no conflicts can occur.
     * Commit:  Copy all temp keys to their final destination keys.
     * Cleanup: Delete original source keys (skipping any that are also a final
     *          destination), then delete all temp keys.
     *
     * If staging fails, all temp keys are deleted and the original keys are untouched.
     * If commit fails, all completed final keys and temp keys are deleted.
     */
    fun renameS3Files(
        client: MinioClient,
        bucket: String,
        renameInstructions: List<RenameInstruction>,
        prefix: String? = null
    ) {
        val effectiveRenames = renameInstructions.filter { it.originalName != it.newName }

        if (effectiveRenames.isEmpty()) {
            logger.debug("No effective S3 renames to process")
            return
        }

        logger.info("Starting S3 rename of ${effectiveRenames.size} instructions in bucket '$bucket'")

        val stagedForRollback = mutableListOf<Pair<String, String>>()
        val stagedMoves = mutableListOf<Pair<String, String>>()

        // Staging: Copy all originals to temp/stage
        // This ensures swap/cycle conflicts (e.g. A→B and B→A) are handled safely.
        try {
            for (instruction in effectiveRenames) {
                val sourceId = extractIdFromFilename(instruction.originalName)
                    ?: throw IllegalArgumentException("Could not extract sourceId from '${instruction.originalName}'")
                val targetId = extractIdFromFilename(instruction.newName)
                    ?: throw IllegalArgumentException("Could not extract targetId from '${instruction.newName}'")

                for (representation in KEY_REPRESENTATIONS) {
                    val sourceKey = buildKey(prefix, sourceId, representation, instruction.originalName)
                    val finalKey = buildKey(prefix, targetId, representation, instruction.newName)
                    // Each temp key is unique per file to avoid naming conflicts
                    val stagedKey = "tmp_${UUIDv7.randomUUID()}_${instruction.originalName}"

                    if (!keyExists(client, bucket, sourceKey)) {
                        throw IllegalStateException(
                            "Source key not found in bucket '$bucket': '$sourceKey'. " +
                                    "S3 structure may be out of sync with disk. Aborting rename."
                        )
                    }

                    copyObjectWithinBucket(client, bucket, sourceKey, stagedKey)
                    stagedForRollback.add(stagedKey to sourceKey)
                    stagedMoves.add(stagedKey to finalKey)
                }
            }
        } catch (e: Exception) {
            logger.error("Staging failed after ${stagedForRollback.size} keys copied, rolling back all temp keys", e)
            rollbackDelete(client, bucket, stagedForRollback.map { it.first })
            throw e
        }

        //Commit: Copy temp keys to final destinations
        // All originals are safely staged at this point, so no swap conflicts possible.
        val completedFinals = mutableListOf<String>()
        try {
            for ((stagedKey, finalKey) in stagedMoves) {
                copyObjectWithinBucket(client, bucket, stagedKey, finalKey)
                completedFinals.add(finalKey)
                logger.debug("Committed temp key '$stagedKey' to final destination '$finalKey'")
            }
        } catch (e: Exception) {
            logger.error("Commit failed after ${completedFinals.size} of ${stagedMoves.size} keys written, rolling back", e)
            rollbackDelete(client, bucket, completedFinals)
            rollbackDelete(client, bucket, stagedForRollback.map { it.first })
            throw e
        }

        // Cleanup: Delete original source keys and temp keys
        // Only delete originals that are NOT also a final destination.
        // Example: in a swap A→B, B→A, both A and B appear as both source and destination,
        // so neither should be deleted — the final copy already wrote the correct content there.
        val finalKeys = stagedMoves.map { it.second }.toSet()
        val originalKeysToDelete = stagedForRollback
            .map { it.second }
            .filter { originalKey -> originalKey !in finalKeys }

        logger.debug("Deleting ${originalKeysToDelete.size} original keys and ${stagedForRollback.size} temp keys from bucket '$bucket'")

        batchDelete(client, bucket, originalKeysToDelete)
        batchDelete(client, bucket, stagedForRollback.map { it.first })

        logger.info("S3 rename complete — ${effectiveRenames.size} instructions processed, ${originalKeysToDelete.size} original keys deleted from bucket '$bucket'")
    }

    /**
     * Returns true if the given key exists in the bucket, false if not found.
     * Any other S3 error (permissions, network) is rethrown.
     */
    private fun keyExists(client: MinioClient, bucket: String, key: String): Boolean =
        try {
            client.statObject(
                StatObjectArgs.builder().bucket(bucket).`object`(key).build()
            )
            true
        } catch (e: ErrorResponseException) {
            if (e.errorResponse().code() == "NoSuchKey") false else throw e
        }

    /**
     * Copies a single object within the same bucket.
     */
    private fun copyObjectWithinBucket(client: MinioClient, bucket: String, sourceKey: String, destKey: String) {
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
    private fun rollbackDelete(client: MinioClient, bucket: String, keys: List<String>) {
        keys.forEach { key ->
            try {
                client.removeObject(
                    RemoveObjectArgs.builder().bucket(bucket).`object`(key).build()
                )
                logger.warn("Rollback deleted temp key '$key' from bucket '$bucket'")
            } catch (e: Exception) {
                logger.error("Rollback failed to delete '$key' from bucket '$bucket'", e)
            }
        }
    }

    /**
     * Deletes a list of keys in batches of 1000 (S3 API limit per request).
     * Used for bulk cleanup of original and temp keys after a successful rename.
     */
    private fun batchDelete(client: MinioClient, bucket: String, keys: List<String>) {
        if (keys.isEmpty()) {
            logger.debug("No keys to delete in bucket '$bucket'")
            return
        }

        val deleteObjects = keys.map { DeleteObject(it) }
        val results = client.removeObjects(
            RemoveObjectsArgs.builder()
                .bucket(bucket)
                .objects(deleteObjects)
                .build()
        )

        // RemoveObjects is lazy — must iterate results to trigger deletion
        results.forEach { result ->
            val error = result.get()
            logger.error("Failed to delete key '${error.objectName()}' from bucket '$bucket': ${error.message()}")
        }

        logger.debug("Deleted ${keys.size} keys from bucket '$bucket'")
    }

    private fun buildKey(prefix: String?, itemId: String, representation: String, filename: String): String =
        if (prefix.isNullOrBlank()) "$itemId/$representation/$filename"
        else "$prefix/$itemId/$representation/$filename"
}