package no.nb.nifi.tekst.exceptions

class MetsCreateException : Exception {
    constructor() : super()

    constructor(message: String?, cause: Throwable?) : super(message, cause)

    constructor(message: String?) : super(message)

    constructor(cause: Throwable?) : super(cause)
}