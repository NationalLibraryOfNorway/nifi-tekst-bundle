package no.nb.utils

import no.nb.models.RenameInstruction
import no.nb.models.StagedMove
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.UUID

object RenameUtils {

    private val logger = LoggerFactory.getLogger(RenameUtils::class.java)

    private fun extractIdFromFilename(filename: String): String? {
        // Updated regex to support underscores in itemId
        val regex = Regex("""^([a-zA-Z0-9\-_]+)_\d+""")
        return regex.find(filename)?.groups?.get(1)?.value
    }

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

    fun renameAll(baseDir: Path, renameInstructions: List<RenameInstruction>) {
        val effectiveRenames: List<RenameInstruction> = renameInstructions.filter { it.originalName != it.newName }
        if (effectiveRenames.isEmpty()) {
            logger.debug("No effective renames to process")
            return
        }

        val tempDir = baseDir.resolve("temp_conflicts_${UUID.randomUUID()}")
        Files.createDirectories(tempDir)

        data class Staged(val original: Path, val temp: Path)
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
                        val tempFile = tempDir.resolve("${UUID.randomUUID()}_${renameInstruction.originalName}")
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

                Files.move(move.tempFile, finalPath, StandardCopyOption.REPLACE_EXISTING)
                completedMoves.add(move.tempFile to finalPath)
                logger.debug("Moved ${move.tempFile} to $finalPath")
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
            deleteTempDir(tempDir)
            throw e
        }

        deleteTempDir(tempDir)
    }
}