package no.nb.nifi.tekst.util

/**
 * Centralized XML-related constants used throughout the application.
 * Includes namespace URIs, schema locations, and other XML constants.
 * This ensures consistency and reduces duplication of constant definitions.
 */
object XmlConstants {
    /**
     * XML Namespace (xmlns) namespace
     * Used for declaring XML namespaces
     */
    const val XMLNS = "http://www.w3.org/2000/xmlns/"

    /**
     * METS (Metadata Encoding and Transmission Standard) namespace
     * Used for METS document structure
     */
    const val METS = "http://www.loc.gov/METS/"

    /**
     * METS 2 (Metadata Encoding and Transmission Standard, version 2) namespace
     * Used for METS 2 document structure
     */
    const val METS2 = "http://www.loc.gov/METS/v2"

    /**
     * MIX (Metadata for Images in XML) v1.0 namespace
     * Used for technical metadata about images
     */
    const val MIX = "http://www.loc.gov/mix/v10"

    /**
     * MIX (Metadata for Images in XML) v2.0 namespace
     * Used for technical metadata about images
     */
    const val MIX2 = "http://www.loc.gov/mix/v20"

    /**
     * XLink (XML Linking Language) namespace
     * Used for expressing links between resources
     */
    const val XLINK = "http://www.w3.org/1999/xlink"

    /**
     * XML Schema Instance namespace
     * Used for schema location and type information
     */
    const val XSI = "http://www.w3.org/2001/XMLSchema-instance"

    /**
     * JHOVE (JSTOR/Harvard Object Validation Environment) namespace
     * Used for digital object validation and metadata
     */
    const val JHOVE = "http://schema.openpreservation.org/ois/xml/ns/jhove"

    /**
     * ALTO (Analyzed Layout and Text Object) namespace
     * Used for OCR and text layout information
     */
    const val ALTO = "http://www.loc.gov/standards/alto/ns-v2#"

    // Schema locations

    /**
     * METS XSD schema location
     */
    const val METS_XSD = "http://www.loc.gov/standards/mets/mets.xsd"

    /**
     * METS 2 XSD schema location
     */
    const val METS2_XSD = "https://loc.gov/standards/mets/mets2.xsd"

    /**
     * MIX v1.0 XSD schema location
     */
    const val MIX_XSD = "http://www.loc.gov/standards/mix/mix10/mix10.xsd"

    /**
     * MIX v2.0 XSD schema location
     */
    const val MIX2_XSD = "https://www.loc.gov/standards/mix/mix.xsd"
}
