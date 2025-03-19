package no.nb.nifi.tekst.exception

/**
 * This exception is thrown from a MetsCreate task if the creation of the mets file fails.
 * @author janarne
 */
class MetsCreateException : Exception {
    /**
     * Creates a new instance of MetsCreateException
     */
    constructor() : super()

    /**
     * Creates a new instance of MetsCreateException
     * @param message The message for this exception.
     * @param cause The cause for this exception.
     */
    constructor(message: String?, cause: Throwable?) : super(message, cause)

    /**
     * Creates a new instance of MetsCreateException
     * @param message The message for this exception.
     */
    constructor(message: String?) : super(message)

    /**
     * Creates a new instance of MetsCreateException
     * @param cause The cause for this exception.
     */
    constructor(cause: Throwable?) : super(cause)
}

