import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

fun renameAll(
    baseDir: Path,
    entries: List<Entry>
) {
    // Map originalName -> Entry for fast lookup
    val renameMap = entries.associateBy { it.originalName }

    // Tracks files already processed
    val visited = mutableSetOf<String>()

    // Process each rename request
    for (entry in entries) {
        renameRecursive(
            fileName = entry.originalName,
            renameMap = renameMap,
            visited = visited,
            baseDir = baseDir
        )
    }
}

private fun renameRecursive(
    fileName: String,
    renameMap: Map<String, Entry>,
    visited: MutableSet<String>,
    baseDir: Path
) {
    if (visited.contains(fileName)) return

    val entry = renameMap[fileName] ?: return
    val targetName = entry.newName

    // Already correct → nothing to do
    if (fileName == targetName) {
        visited.add(fileName)
        return
    }

    // If target itself must be renamed, resolve it first
    if (renameMap.containsKey(targetName)) {
        renameRecursive(
            fileName = targetName,
            renameMap = renameMap,
            visited = visited,
            baseDir = baseDir
        )
    }

    // Perform safe rename in both directories
    renameInItemDirectories(baseDir, fileName, targetName)

    visited.add(fileName)
}
