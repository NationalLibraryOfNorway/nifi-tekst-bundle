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

    fun renameAll(baseDir: Path, renameInstructions: List<RenameInstruction>) {
        val effectiveRenames = renameInstructions
            .filter { it.originalName != it.newName }

        if (effectiveRenames.isEmpty()) {
            logger.debug("No effective renames to process")
            return
        }

        // Phase 1: move all files to a unique temp directory
        val tempDir = baseDir.resolve("temp_conflicts_${UUID.randomUUID()}")
        Files.createDirectories(tempDir)

        val stagedMoves = mutableListOf<StagedMove>()

        for (renameInstruction in effectiveRenames) {
            val sourceId = renameInstruction.originalName.substringBefore("_000")
            val targetId = renameInstruction.newName.substringBefore("_000")

            val sourceDirs = listOf(
                baseDir.resolve("$sourceId/representations/access/data"),
                baseDir.resolve("$sourceId/representations/primary/data")
            )

            val targetDirs = listOf(
                baseDir.resolve("$targetId/representations/access/data"),
                baseDir.resolve("$targetId/representations/primary/data")
            )

            sourceDirs.zip(targetDirs).forEach { (sourceDir, targetDir) ->
                if (!Files.exists(sourceDir)) {
                    logger.warn("Source directory does not exist: $sourceDir")
                    return@forEach
                }

                val sourceFile = sourceDir.resolve(renameInstruction.originalName)
                if (!Files.exists(sourceFile)) {
                    logger.warn("File not found: $sourceFile")
                    return@forEach
                }

                try {
                    //Moves to temporary directory
                    val tempFile = tempDir.resolve("${UUID.randomUUID()}_${renameInstruction.originalName}")
                    Files.move(sourceFile, tempFile, StandardCopyOption.REPLACE_EXISTING)

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
                }
            }
        }

        // Phase 2: Move/rename staged files to final destinations
        for (move in stagedMoves) {
            val finalPath = move.targetDir.resolve(move.finalName)

            try {
                if (Files.exists(finalPath)) {
                    val backup = move.targetDir.resolve("${UUID.randomUUID()}_${move.finalName}")
                    Files.move(finalPath, backup, StandardCopyOption.REPLACE_EXISTING)
                    logger.debug("Backed up existing file to $backup")
                }

                Files.move(move.tempFile, finalPath, StandardCopyOption.REPLACE_EXISTING)
            } catch (e: Exception) {
                logger.error("Failed final move for ${move.tempFile}", e)
            }
        }

        // Cleanup
        try {
            Files.deleteIfExists(tempDir)
        } catch (e: Exception) {
            logger.warn("Temp directory not empty or could not be deleted: $tempDir", e)
        }
    }
}
