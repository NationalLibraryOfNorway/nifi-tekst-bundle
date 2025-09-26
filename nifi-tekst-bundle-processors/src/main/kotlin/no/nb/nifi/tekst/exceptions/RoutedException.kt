package no.nb.nifi.tekst.exceptions

import org.apache.nifi.processor.Relationship

class RoutedException(
    val relationship: Relationship,
    val penalize: Boolean? = false,
    message: String,
    cause: Throwable? = null
): Exception(message, cause)
