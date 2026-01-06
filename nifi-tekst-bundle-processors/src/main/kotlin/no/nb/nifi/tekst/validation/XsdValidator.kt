package no.nb.nifi.tekst.validation

import org.w3c.dom.ls.LSInput
import org.w3c.dom.ls.LSResourceResolver
import org.xml.sax.SAXException
import java.io.File
import java.io.StringReader
import java.nio.file.Files
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory

/**
 * Utility class for validating XML against XSD schemas.
 * Uses XSD files from classpath resources to avoid HTTP calls during validation.
 */
object XsdValidator {

    private const val METS_XSD_RESOURCE = "/xsd/mets.xsd"
    private const val METS2_XSD_RESOURCE = "/xsd/mets2.xsd"
    private const val MIX_XSD_RESOURCE = "/xsd/mix10.xsd"
    private const val MIX2_XSD_RESOURCE = "/xsd/mix20.xsd"
    private const val XLINK_XSD_RESOURCE = "/xsd/xlink.xsd"
    private const val JHOVE_XSD_RESOURCE = "/xsd/jhove.xsd"

    /**
     * Custom resource resolver that maps remote XSD URLs to local classpath resources.
     */
    private class ClasspathResourceResolver : LSResourceResolver {
        private val urlToResource = mapOf(
            "http://www.loc.gov/standards/xlink/xlink.xsd" to XLINK_XSD_RESOURCE,
            "http://www.w3.org/1999/xlink.xsd" to XLINK_XSD_RESOURCE,
            "http://www.loc.gov/standards/mets/mets.xsd" to METS_XSD_RESOURCE,
            "http://www.loc.gov/standards/mets/mets2.xsd" to METS2_XSD_RESOURCE,
            "https://loc.gov/standards/mets/mets2.xsd" to METS2_XSD_RESOURCE,
            "http://www.loc.gov/standards/mix/mix10/mix10.xsd" to MIX_XSD_RESOURCE,
            "http://www.loc.gov/standards/mix/mix20/mix20.xsd" to MIX2_XSD_RESOURCE,
            "https://schema.openpreservation.org/ois/xml/xsd/jhove/1.10/jhove.xsd" to JHOVE_XSD_RESOURCE,
            "http://schema.openpreservation.org/ois/xml/xsd/jhove/1.10/jhove.xsd" to JHOVE_XSD_RESOURCE,
            "https://schema.openpreservation.org/ois/xml/xsd/jhove/1.8/jhove.xsd" to JHOVE_XSD_RESOURCE,
            "http://schema.openpreservation.org/ois/xml/xsd/jhove/1.8/jhove.xsd" to JHOVE_XSD_RESOURCE
        )

        override fun resolveResource(
            type: String?,
            namespaceURI: String?,
            publicId: String?,
            systemId: String?,
            baseURI: String?
        ): LSInput? {
            val resourcePath = urlToResource[systemId] ?: return null
            val content = javaClass.getResourceAsStream(resourcePath)?.bufferedReader()?.readText() ?: return null
            return SimpleLSInput(
                publicId = publicId,
                systemId = systemId,
                baseURI = baseURI,
                stringData = content
            )
        }
    }

    /**
     * Simple implementation of LSInput for providing local file content.
     */
    private class SimpleLSInput(
        private val publicId: String?,
        private val systemId: String?,
        private val baseURI: String?,
        private val stringData: String?
    ) : LSInput {
        override fun getPublicId(): String? = publicId
        override fun setPublicId(publicId: String?) {}
        override fun getSystemId(): String? = systemId
        override fun setSystemId(systemId: String?) {}
        override fun getBaseURI(): String? = baseURI
        override fun setBaseURI(baseURI: String?) {}
        override fun getCharacterStream(): java.io.Reader? = stringData?.reader()
        override fun setCharacterStream(characterStream: java.io.Reader?) {}
        override fun getByteStream(): java.io.InputStream? = null
        override fun setByteStream(byteStream: java.io.InputStream?) {}
        override fun getStringData(): String? = stringData
        override fun setStringData(stringData: String?) {}
        override fun getEncoding(): String? = "UTF-8"
        override fun setEncoding(encoding: String?) {}
        override fun getCertifiedText(): Boolean = false
        override fun setCertifiedText(certifiedText: Boolean) {}
    }

    /**
     * Validates an XML file against the METS and MIX schemas.
     * @param xmlFile The XML file to validate
     * @return ValidationResult with success status and any error messages
     */
    fun validateMets(xmlFile: File): ValidationResult {
        return validateMets(xmlFile.readText())
    }

    /**
     * Validates XML content against the METS and MIX schemas.
     * @param xmlContent The XML content to validate
     * @return ValidationResult with success status and any error messages
     */
    fun validateMets(xmlContent: String): ValidationResult {
        return validate(xmlContent, listOf(XLINK_XSD_RESOURCE, MIX_XSD_RESOURCE, METS_XSD_RESOURCE))
    }

    /**
     * Validates an XML file against the METS 2 schema.
     * @param xmlFile The XML file to validate
     * @return ValidationResult with success status and any error messages
     */
    fun validateMets2(xmlFile: File): ValidationResult {
        return validateMets2(xmlFile.readText())
    }

    /**
     * Validates XML content against the METS 2 schema.
     * METS 2 uses namespace http://www.loc.gov/METS/v2
     * @param xmlContent The XML content to validate
     * @return ValidationResult with success status and any error messages
     */
    fun validateMets2(xmlContent: String): ValidationResult {
        return validate(xmlContent, listOf(METS2_XSD_RESOURCE))
    }

    /**
     * Validates an XML file against the JHOVE schema.
     * @param xmlFile The XML file to validate
     * @return ValidationResult with success status and any error messages
     */
    fun validateJhove(xmlFile: File): ValidationResult {
        return validateJhove(xmlFile.readText())
    }

    /**
     * Validates XML content against the JHOVE schema.
     * @param xmlContent The XML content to validate
     * @return ValidationResult with success status and any error messages
     */
    fun validateJhove(xmlContent: String): ValidationResult {
        return validate(xmlContent, listOf(JHOVE_XSD_RESOURCE))
    }

    /**
     * Validates an XML file against the MIX 1.0 schema.
     * @param xmlFile The XML file to validate
     * @return ValidationResult with success status and any error messages
     */
    fun validateMix(xmlFile: File): ValidationResult {
        return validateMix(xmlFile.readText())
    }

    /**
     * Validates XML content against the MIX 1.0 schema.
     * @param xmlContent The XML content to validate
     * @return ValidationResult with success status and any error messages
     */
    fun validateMix(xmlContent: String): ValidationResult {
        return validate(xmlContent, listOf(MIX_XSD_RESOURCE))
    }

    /**
     * Validates an XML file against the MIX 2.0 schema.
     * @param xmlFile The XML file to validate
     * @return ValidationResult with success status and any error messages
     */
    fun validateMix2(xmlFile: File): ValidationResult {
        return validateMix2(xmlFile.readText())
    }

    /**
     * Validates XML content against the MIX 2.0 schema.
     * MIX 2.0 uses namespace http://www.loc.gov/mix/v20
     * @param xmlContent The XML content to validate
     * @return ValidationResult with success status and any error messages
     */
    fun validateMix2(xmlContent: String): ValidationResult {
        return validate(xmlContent, listOf(MIX2_XSD_RESOURCE))
    }

    /**
     * Extracts MIX elements from a METS document and validates them against the MIX 2.0 schema.
     * This is useful for validating MIX metadata embedded within METS files.
     * @param metsContent The METS XML content containing MIX metadata
     * @return ValidationResult with success status and any error messages for all MIX elements
     */
    fun validateMix2InMets(metsContent: String): ValidationResult {
        return validateMixInMets(metsContent, MIX2_XSD_RESOURCE, "http://www.loc.gov/mix/v20")
    }

    /**
     * Extracts MIX elements from a METS document and validates them against the MIX 1.0 schema.
     * This is useful for validating MIX metadata embedded within METS files.
     * @param metsContent The METS XML content containing MIX metadata
     * @return ValidationResult with success status and any error messages for all MIX elements
     */
    fun validateMixInMets(metsContent: String): ValidationResult {
        return validateMixInMets(metsContent, MIX_XSD_RESOURCE, "http://www.loc.gov/mix/v10")
    }

    /**
     * Internal method to extract and validate MIX elements from METS content.
     */
    private fun validateMixInMets(metsContent: String, mixXsdResource: String, mixNamespace: String): ValidationResult {
        return try {
            val docBuilder = javax.xml.parsers.DocumentBuilderFactory.newInstance().apply {
                isNamespaceAware = true
            }.newDocumentBuilder()

            val doc = docBuilder.parse(java.io.ByteArrayInputStream(metsContent.toByteArray()))

            // Find all mix:mix elements
            val mixElements = doc.getElementsByTagNameNS(mixNamespace, "mix")
            if (mixElements.length == 0) {
                return ValidationResult(isValid = false, errors = listOf("No MIX elements found in METS document with namespace $mixNamespace"))
            }

            val allErrors = mutableListOf<String>()

            // Validate each MIX element separately
            for (i in 0 until mixElements.length) {
                val mixElement = mixElements.item(i)

                // Serialize the MIX element to a standalone XML document
                val transformer = javax.xml.transform.TransformerFactory.newInstance().newTransformer()
                transformer.setOutputProperty(javax.xml.transform.OutputKeys.OMIT_XML_DECLARATION, "no")
                transformer.setOutputProperty(javax.xml.transform.OutputKeys.INDENT, "yes")

                val stringWriter = java.io.StringWriter()
                transformer.transform(
                    javax.xml.transform.dom.DOMSource(mixElement),
                    javax.xml.transform.stream.StreamResult(stringWriter)
                )

                val mixXml = stringWriter.toString()

                // Validate the extracted MIX element
                val result = validate(mixXml, listOf(mixXsdResource))
                if (!result.isValid) {
                    allErrors.addAll(result.errors.map { "MIX element ${i + 1}: $it" })
                }
            }

            if (allErrors.isEmpty()) {
                ValidationResult(isValid = true, errors = emptyList())
            } else {
                ValidationResult(isValid = false, errors = allErrors)
            }
        } catch (e: Exception) {
            ValidationResult(isValid = false, errors = listOf("Failed to extract MIX elements: ${e.message}"))
        }
    }

    /**
     * Generic validation method that validates XML against multiple XSD schemas.
     * @param xmlContent The XML content to validate
     * @param xsdResources List of XSD resource paths to validate against (order matters for imports)
     * @return ValidationResult with success status and any error messages
     */
    private fun validate(xmlContent: String, xsdResources: List<String>): ValidationResult {
        return try {
            val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
            schemaFactory.resourceResolver = ClasspathResourceResolver()

            // Load XSD files from classpath and create temp files for schema factory
            val tempFiles = mutableListOf<File>()
            try {
                val schemaSources = xsdResources.map { resourcePath ->
                    val inputStream = javaClass.getResourceAsStream(resourcePath)
                        ?: throw IllegalStateException("Could not load XSD from classpath: $resourcePath")

                    val tempFile = Files.createTempFile("xsd-", ".xsd").toFile()
                    tempFile.deleteOnExit()
                    tempFiles.add(tempFile)

                    inputStream.use { stream ->
                        tempFile.writeBytes(stream.readBytes())
                    }
                    StreamSource(tempFile)
                }.toTypedArray()

                val schema = schemaFactory.newSchema(schemaSources)
                val validator = schema.newValidator()
                validator.resourceResolver = ClasspathResourceResolver()

                validator.validate(StreamSource(StringReader(xmlContent)))

                ValidationResult(isValid = true, errors = emptyList())
            } finally {
                tempFiles.forEach { it.delete() }
            }
        } catch (e: SAXException) {
            ValidationResult(isValid = false, errors = listOf(e.message ?: "Unknown validation error"))
        } catch (e: Exception) {
            ValidationResult(isValid = false, errors = listOf("Validation failed: ${e.message}"))
        }
    }
}

/**
 * Result of XSD validation.
 */
data class ValidationResult(
    val isValid: Boolean,
    val errors: List<String>
) {
    /**
     * Throws an AssertionError if validation failed.
     */
    fun assertValid() {
        if (!isValid) {
            throw AssertionError("XML validation failed:\n${errors.joinToString("\n")}")
        }
    }

    /**
     * Returns the error message if validation failed, or null if valid.
     */
    fun getErrorMessage(): String? {
        return if (!isValid) errors.joinToString("; ") else null
    }
}

