package no.nb.models

import java.nio.file.Path

data class Staged(
    val original: Path,
    val temp: Path
)
