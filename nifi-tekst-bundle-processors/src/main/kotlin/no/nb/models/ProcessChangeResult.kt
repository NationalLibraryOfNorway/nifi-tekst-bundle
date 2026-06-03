package no.nb.models

data class ProcessChangesResult(
    val renameInstructions: List<RenameInstruction>,
    val itemIds: List<String>,
    val items: List<Map<String, Any>>
)