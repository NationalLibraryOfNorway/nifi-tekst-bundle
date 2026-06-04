package no.nb.utils

import org.slf4j.LoggerFactory
import java.nio.file.Path

/**
 * Shared validation helpers for filesystem and S3 path components.
 *
 * Centralizes the rules for what counts as a "safe" name (no path traversal,
 * no separators, no NUL bytes) and for asserting that a resolved path stays
 * inside a configured base directory.
 */
object PathSafety {

    private val logger = LoggerFactory.getLogger(PathSafety::class.java)

    /**
     * Returns true if [name] is safe to use as a single path component.
     * Disallows blank names, parent-directory references (`..`), path separators,
     * and NUL bytes.
     */
    fun isSafeName(name: String): Boolean {
        val safe = name.isNotBlank() &&
                !name.contains("..") &&
                !name.contains("/") &&
                !name.contains("\\") &&
                !name.contains("\u0000")
        if (!safe) {
            logger.warn("Validation failed for name: '{}'", name)
        }
        return safe
    }

    /**
     * Ensures the resolved path is strictly within the base directory.
     * Throws SecurityException if path traversal is detected.
     */
    fun requireWithinBaseDir(baseDirPath: Path, resolvedPath: Path) {
        val normalized = resolvedPath.normalize()
        if (!normalized.startsWith(baseDirPath.normalize())) {
            logger.error("Path traversal detected: {} is outside base directory {}", resolvedPath, baseDirPath)
            throw IllegalArgumentException("Path traversal detected: $resolvedPath is outside base directory $baseDirPath")
        }
    }
}

