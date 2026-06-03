package no.nb.utils

import no.nb.models.RenameInstruction
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import no.nb.utils.RenameUtils.extractIdFromFilename

object RenameDiskUtils {

    private val logger = LoggerFactory.getLogger(RenameDiskUtils::class.java)

    private data class StagedFile(
        val original: Path,
        val temp: Path,
        val targetDir: Path,
        val finalName: String
    ) {
        val finalPath: Path get() = targetDir.resolve(finalName).normalize()
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

    fun renameFilesOnDisk(baseDir: Path, renameInstructions: List<RenameInstruction>) {
        val effectiveRenames = renameInstructions.filter { it.originalName != it.newName }
        if (effectiveRenames.isEmpty()) {
            logger.debug("No effective renames to process")
            return
        }

        val tempDir = baseDir.resolve("temp_conflicts_${UUIDv7.randomUUID()}")
        Files.createDirectories(tempDir)
        val staged = mutableListOf<StagedFile>()

        try {
            // Phase 1: move each source file to a temp location
            for (instruction in effectiveRenames) {
                val sourceId = extractIdFromFilename(instruction.originalName)
                    ?: throw IllegalArgumentException("Could not extract sourceId from ${instruction.originalName}")
                val targetId = extractIdFromFilename(instruction.newName)
                    ?: throw IllegalArgumentException("Could not extract targetId from ${instruction.newName}")

                listOf("access", "primary").forEach { rep ->
                    val sourceDir = baseDir.resolve("$sourceId/representations/$rep/data").normalize()
                    val targetDir = baseDir.resolve("$targetId/representations/$rep/data").normalize()
                    requireWithinBaseDir(baseDir, sourceDir)
                    requireWithinBaseDir(baseDir, targetDir)

                    if (!Files.exists(sourceDir)) {
                        logger.warn("Source directory does not exist: $sourceDir")
                        return@forEach
                    }
                    val sourceFile = sourceDir.resolve(instruction.originalName).normalize()
                    requireWithinBaseDir(baseDir, sourceFile)
                    if (!Files.exists(sourceFile)) {
                        logger.warn("File not found: $sourceFile")
                        return@forEach
                    }

                    val tempFile = tempDir.resolve("${UUIDv7.randomUUID()}_${instruction.originalName}")
                    Files.move(sourceFile, tempFile, StandardCopyOption.REPLACE_EXISTING)
                    staged.add(StagedFile(sourceFile, tempFile, targetDir, instruction.newName))
                }
            }

            // Phase 2: move staged temp files to their final destinations
            for (file in staged) {
                requireWithinBaseDir(baseDir, file.finalPath)
                Files.createDirectories(file.targetDir)
                if (Files.exists(file.finalPath)) {
                    logger.warn("Target file already exists and will be overwritten: ${file.finalPath}")
                }
                Files.move(file.temp, file.finalPath, StandardCopyOption.REPLACE_EXISTING)
                logger.debug("Moved {} to {}", file.temp, file.finalPath)
            }
        } catch (e: Exception) {
            logger.error("Rename failed, rolling back", e)
            for (file in staged.asReversed()) {
                val current = if (Files.exists(file.finalPath)) file.finalPath else file.temp
                runCatching { Files.move(current, file.original, StandardCopyOption.REPLACE_EXISTING) }
                    .onFailure { logger.error("Failed to restore ${file.original}", it) }
            }
            throw e
        } finally {
            deleteTempDir(tempDir)
        }
    }
}