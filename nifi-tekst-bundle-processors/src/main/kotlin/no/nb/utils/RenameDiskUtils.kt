package no.nb.utils

import no.nb.models.RenameInstruction
import no.nb.models.Staged
import no.nb.models.StagedMove
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import no.nb.utils.RenameUtils.extractIdFromFilename

object RenameDiskUtils {

    private val logger = LoggerFactory.getLogger(RenameDiskUtils::class.java)

    private fun requireWithinBaseDir(baseDir: Path, path: Path) {
        require(path.normalize().startsWith(baseDir.normalize())) {
            "Path traversal detected: $path is outside base directory $baseDir"
        }
    }

    private fun deleteTempDir(tempDir: Path) {
        try {
            tempDir.toFile().deleteRecursively()
        } catch (e: Exception) {
            logger.warn("Failed to delete temp directory: $tempDir", e)
        }
    }

    fun renameFilsOnDisk(baseDir: Path, renameInstructions: List<RenameInstruction>) {
        val effectiveRenames: List<RenameInstruction> = renameInstructions.filter { it.originalName != it.newName }
        if (effectiveRenames.isEmpty()) {
            logger.debug("No effective renames to process")
            return
        }

        val tempDir = baseDir.resolve("temp_conflicts_${UUIDv7.randomUUID()}")
        Files.createDirectories(tempDir)

        val stagedForRollback = mutableListOf<Staged>()
        val stagedMoves = mutableListOf<StagedMove>()

        try {
            for (renameInstruction in effectiveRenames) {
                val sourceId = extractIdFromFilename(renameInstruction.originalName)
                    ?: throw IllegalArgumentException("Could not extract sourceId from ${renameInstruction.originalName}")
                val targetId = extractIdFromFilename(renameInstruction.newName)
                    ?: throw IllegalArgumentException("Could not extract targetId from ${renameInstruction.newName}")

                val sourceDirs = listOf(
                    baseDir.resolve("$sourceId/representations/access/data").normalize(),
                    baseDir.resolve("$sourceId/representations/primary/data").normalize()
                )
                val targetDirs = listOf(
                    baseDir.resolve("$targetId/representations/access/data").normalize(),
                    baseDir.resolve("$targetId/representations/primary/data").normalize()
                )

                // Validate all paths stay within baseDir
                sourceDirs.forEach { requireWithinBaseDir(baseDir, it) }
                targetDirs.forEach { requireWithinBaseDir(baseDir, it) }

                sourceDirs.zip(targetDirs).forEach { (sourceDir, targetDir) ->
                    if (!Files.exists(sourceDir)) {
                        logger.warn("Source directory does not exist: $sourceDir")
                        return@forEach
                    }
                    val sourceFile = sourceDir.resolve(renameInstruction.originalName).normalize()
                    requireWithinBaseDir(baseDir, sourceFile)

                    if (!Files.exists(sourceFile)) {
                        logger.warn("File not found: $sourceFile")
                        return@forEach
                    }
                    try {
                        val tempFile = tempDir.resolve("${UUIDv7.randomUUID()}_${renameInstruction.originalName}")
                        Files.move(sourceFile, tempFile, StandardCopyOption.REPLACE_EXISTING)
                        stagedForRollback.add(Staged(sourceFile, tempFile))
                        Files.createDirectories(targetDir)
                        stagedMoves.add(
                            StagedMove(
                                tempFile = tempFile,
                                targetDir = targetDir,
                                finalName = renameInstruction.newName
                            )
                        )
                    } catch (e: Exception) {
                        logger.error("Failed to stage file: $sourceFile", e)
                        throw e
                    }
                }
            }
        } catch (e: Exception) {
            // Rollback: move staged files back to original locations
            logger.error("Exception during staging, rolling back staged files", e)
            for (staged in stagedForRollback) {
                try {
                    if (Files.exists(staged.temp)) {
                        Files.move(staged.temp, staged.original, StandardCopyOption.REPLACE_EXISTING)
                    }
                } catch (ex: Exception) {
                    logger.error("Failed to rollback file ${staged.temp} to ${staged.original}", ex)
                }
            }
            deleteTempDir(tempDir)
            throw e
        }

        // Phase 2: Move staged files to final destinations with rollback support
        val completedMoves = mutableListOf<Pair<Path, Path>>() // temp -> final
        try {
            for (move in stagedMoves) {
                val finalPath = move.targetDir.resolve(move.finalName).normalize()
                requireWithinBaseDir(baseDir, finalPath)

                if (Files.exists(finalPath)) {
                    logger.warn("Target file already exists and will be overwritten: $finalPath")
                }
                Files.move(move.tempFile, finalPath, StandardCopyOption.REPLACE_EXISTING)
                completedMoves.add(move.tempFile to finalPath)
                logger.debug("Moved {} to {}", move.tempFile, finalPath)
            }
        } catch (e: Exception) {
            // Rollback phase 2: move completed files back to temp locations
            logger.error("Exception during final moves, attempting rollback of phase 2", e)
            for ((temp, final) in completedMoves.asReversed()) {
                try {
                    if (Files.exists(final)) {
                        Files.move(final, temp, StandardCopyOption.REPLACE_EXISTING)
                        logger.warn("Rolled back $final to $temp")
                    }
                } catch (ex: Exception) {
                    logger.error("Failed to rollback $final to $temp", ex)
                }
            }
            // Restore all staged files back to their original source locations
            for (staged in stagedForRollback.asReversed()) {
                try {
                    if (Files.exists(staged.temp)) {
                        Files.move(staged.temp, staged.original, StandardCopyOption.REPLACE_EXISTING)
                        logger.warn("Restored ${staged.temp} to original location ${staged.original}")
                    }
                } catch (ex: Exception) {
                    logger.error("Failed to restore ${staged.temp} to ${staged.original}", ex)
                }
            }
            deleteTempDir(tempDir)
            throw e
        }

        deleteTempDir(tempDir)
    }
}