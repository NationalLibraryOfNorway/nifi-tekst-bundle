package no.nb.models

import java.nio.file.Path

data class StagedMove(
    val tempFile: Path,
    val targetDir: Path,
    val finalName: String
)
