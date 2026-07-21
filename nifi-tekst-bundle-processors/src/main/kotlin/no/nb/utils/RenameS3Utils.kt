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

        // S3 stores only .tif keys. When both .tif and .jp2 exist on disk for the same page,
        // addInstruction produces two rename instructions that normalize to the same S3 operation.
        // Deduplicate by (normalised source key, normalised destination key) so each S3 key is
        // staged exactly once.
        val s3Renames = effectiveRenames.distinctBy { Pair(toS3Filename(it.originalName), toS3Filename(it.newName)) }
        if (s3Renames.size < effectiveRenames.size) {
            logger.debug(
                "Deduplicated {} disk instructions to {} S3 instructions (multiple on-disk formats share one S3 key)",
                effectiveRenames.size, s3Renames.size
            )
        }

        logger.info("Starting S3 rename of ${s3Renames.size} instructions in bucket '$bucket'")

        val staged = mutableListOf<StagedS3File>()

        // Staging: Copy all originals to temp/stage
        // This ensures swap/cycle conflicts (e.g. A→B and B→A) are handled safely.
        try {
            for (instruction in s3Renames) {
                val sourceId = extractIdFromFilename(instruction.originalName)
                    ?: throw IllegalArgumentException("Could not extract sourceId from '${instruction.originalName}'")
                val targetId = extractIdFromFilename(instruction.newName)
                    ?: throw IllegalArgumentException("Could not extract targetId from '${instruction.newName}'")

                var foundInAnyRepresentation = false

                for (representation in KEY_REPRESENTATIONS) {
                    val sourceKey = buildKey(prefix, sourceId, representation, toS3Filename(instruction.originalName))
                    val finalKey = buildKey(prefix, targetId, representation, toS3Filename(instruction.newName))
                    val tempKey = "tmp_${UUIDv7.randomUUID()}_${instruction.originalName}"

                    if (!keyExists(client, bucket, sourceKey)) {
                        logger.debug("Source key not found in representation '$representation', skipping: '$sourceKey'")
                        continue
                    }

                    foundInAnyRepresentation = true
                    copyObjectWithinBucket(client, bucket, sourceKey, tempKey)
                    staged.add(StagedS3File(sourceKey, tempKey, finalKey))
                }

                if (!foundInAnyRepresentation) {
                    throw IllegalStateException(
                        "Source file '${instruction.originalName}' not found in any representation (access or primary) " +
                                "for itemId '$sourceId' in bucket '$bucket'. " +
                                "S3 structure may be out of sync with disk. Aborting rename."
                    )
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

        logger.info("S3 rename complete — ${s3Renames.size} instructions processed, ${originalKeysToDelete.size} original keys deleted from bucket '$bucket'")
    }

    private fun buildKey(prefix: String?, itemId: String, representation: String, filename: String): String =
        if (prefix.isNullOrBlank()) "$itemId/$representation/$filename"
        else "$prefix/$itemId/$representation/$filename"

    /**
     * S3 always stores image files with a .tif extension, regardless of the on-disk format
     * (e.g. .jp2 files on disk have a corresponding .tif key in S3).
     * Normalises .jp2 and .tiff extensions to .tif when building S3 keys.
     */
    private fun toS3Filename(filename: String): String {
        val ext = filename.substringAfterLast('.', "").lowercase()
        return if (ext == "jp2" || ext == "tiff") "${filename.substringBeforeLast('.')}.tif"
        else filename
    }
}