package no.nb.utils

import no.nb.models.Entry
import no.nb.nifi.tekst.processors.ReorderFiles
import java.io.File
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.StandardCopyOption

data class AccessPrimaryFiles(val access: File?, val primary: File?)
private val logger = LoggerFactory.getLogger(ReorderFiles::class.java)

fun renameAll(
    entries: List<Entry>,
    baseDir: File,
) {
    baseDir.mkdirs()

    val visited = mutableSetOf<Int>()
    val tempDir = File(baseDir, "tmp_conflicts")
    tempDir.mkdirs()

    for (i in entries.indices) {
        renameRecursively(
            index = i,
            entries = entries,
            baseDir = baseDir,
            visited = visited,
            tempDir = tempDir
        )
    }
}

fun renameRecursively(
    index: Int,
    entries: List<Entry>,
    baseDir: File,
    visited: MutableSet<Int>,
    tempDir: File
) {
    if (index !in entries.indices) return
    if (!visited.add(index)) return

    val entry = entries[index]
    val originalName = entry.originalName
    val newName = entry.newName

    val (accessSource, primarySource) = getAccessAndPrimary(baseDir, originalName, tempDir)
    val (accessTarget, primaryTarget) = getAccessAndPrimary(baseDir, newName, tempDir)

    // Only check that the source files exist
    if (accessSource == null || !accessSource.exists()) {
        logger.warn("WARNING: Source file not found anywhere: $accessSource")
        return
    }

    //The source and target files are the same, so nothing to do
    if (accessSource == accessTarget || primarySource == primaryTarget) {
        logger.info("Skipping \\${originalName}, already correct.")
        return
    }

    // If target exists, temporarily move it to tempDir to avoid overwriting
    moveToTempDir(accessTarget, "access", tempDir)
    moveToTempDir(primaryTarget, "primary", tempDir)

    moveFiles(accessSource, accessTarget)
    moveFiles(primarySource, primaryTarget)
}

fun getAccessAndPrimary(baseDir: File, originalName: String, tempDir: File? = null): AccessPrimaryFiles {
    val itemId = originalName.substringBefore('_')
    val accessUrl = "access/data"
    val primaryUrl = "primary/data"
    val accessDir = File(baseDir, "$itemId/representations/$accessUrl")
    val primaryDir = File(baseDir, "$itemId/representations/$primaryUrl")

    val accessFile = File(accessDir, originalName)
    val primaryFile = File(primaryDir, originalName)

    val access = when {
        accessFile.exists() -> accessFile
        tempDir != null -> File(tempDir, "access_$originalName").takeIf { it.exists() }
        else -> null
    }
    val primary = when {
        primaryFile.exists() -> primaryFile
        tempDir != null -> File(tempDir, "primary_$originalName").takeIf { it.exists() }
        else -> null
    }

    return AccessPrimaryFiles(
        access = access,
        primary = primary
    )
}

private fun moveFiles(
    source: File?,
    target: File?,
) {
    if (source == null || target == null || !source.exists()) return

    //if the target exists, we move the target to a temp fiale to avoid overwriting
    if (target.exists()) {
        logger.info("Renaming (swap): ${source.name} -> ${target.name}")
        val tempFile = File(target.parent, "${target.name}.tmp_swap")
        Files.move(target.toPath(), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
        Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        Files.move(tempFile.toPath(), source.toPath(), StandardCopyOption.REPLACE_EXISTING)
    } else {
        logger.info("Renaming: ${source.name} -> ${target.name}")
        Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
    }
}

private fun moveToTempDir(target: File?, prefix: String, tempDir: File) {
    if (target?.exists() == true) {
        val tempFile = File(tempDir, "${prefix}_${target.name}")
        logger.info("Temporarily moving ${prefix} conflicting file: ${target.name} -> ${tempFile.name}")
        Files.move(target.toPath(), tempFile.toPath())
    }
}

