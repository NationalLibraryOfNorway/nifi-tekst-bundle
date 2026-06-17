package no.nb.utils

import io.minio.MinioClient
import no.nb.models.RenameInstruction
import no.nb.utils.RenameUtils.extractIdFromFilename
import no.nb.utils.S3Utils.batchDelete
import no.nb.utils.S3Utils.rollbackDelete
import no.nb.utils.S3Utils.copyObjectWithinBucket
import no.nb.utils.S3Utils.keyExists
import org.slf4j.LoggerFactory

object RenameS3Utils {

    private val logger = LoggerFactory.getLogger(RenameS3Utils::class.java)
    private val KEY_REPRESENTATIONS = listOf(
        "representations/access/data",
        "representations/primary/data"
    )

    private data class StagedS3File(val sourceKey: String, val tempKey: String, val finalKey: String)

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

        val staged = mutableListOf<StagedS3File>()

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
                    val tempKey = "tmp_${UUIDv7.randomUUID()}_${instruction.originalName}"

                    if (!keyExists(client, bucket, sourceKey)) {
                        throw IllegalStateException(
                            "Source key not found in bucket '$bucket': '$sourceKey'. " +
                                    "S3 structure may be out of sync with disk. Aborting rename."
                        )
                    }

                    copyObjectWithinBucket(client, bucket, sourceKey, tempKey)
                    staged.add(StagedS3File(sourceKey, tempKey, finalKey))
                }
            }
        } catch (e: Exception) {
            logger.error("Staging failed after ${staged.size} keys copied, rolling back all temp keys", e)
            rollbackDelete(client, bucket, staged.map { it.tempKey })
            throw e
        }

        // Commit: Copy temp keys to final destinations
        // All originals are safely staged at this point, so no swap conflicts possible.
        val completedFinals = mutableListOf<String>()
        try {
            for (file in staged) {
                copyObjectWithinBucket(client, bucket, file.tempKey, file.finalKey)
                completedFinals.add(file.finalKey)
                logger.debug("Committed temp key '${file.tempKey}' to final destination '${file.finalKey}'")
            }
        } catch (e: Exception) {
            logger.error("Commit failed after ${completedFinals.size} of ${staged.size} keys written, rolling back", e)
            rollbackDelete(client, bucket, completedFinals)
            rollbackDelete(client, bucket, staged.map { it.tempKey })
            throw e
        }

        // Cleanup: Delete original source keys and temp keys
        // Only delete originals that are NOT also a final destination.
        // Example: in a swap A→B, B→A, both A and B appear as both source and destination,
        // so neither should be deleted — the final copy already wrote the correct content there.
        val finalKeys = staged.map { it.finalKey }.toSet()
        val originalKeysToDelete = staged
            .map { it.sourceKey }
            .filter { it !in finalKeys }

        logger.debug("Deleting ${originalKeysToDelete.size} original keys and ${staged.size} temp keys from bucket '$bucket'")

        batchDelete(client, bucket, originalKeysToDelete)
        batchDelete(client, bucket, staged.map { it.tempKey })

        logger.info("S3 rename complete — ${effectiveRenames.size} instructions processed, ${originalKeysToDelete.size} original keys deleted from bucket '$bucket'")
    }

    private fun buildKey(prefix: String?, itemId: String, representation: String, filename: String): String =
        if (prefix.isNullOrBlank()) "$itemId/$representation/$filename"
        else "$prefix/$itemId/$representation/$filename"
}