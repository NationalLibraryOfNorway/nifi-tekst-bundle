package no.nb.nifi.tekst.util

import org.w3c.dom.Document
import org.w3c.dom.NodeList
import java.io.File
import java.io.IOException
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory

/**
 * Utility class for XML parsing operations.
 */
object XmlHelper {
    /**
     * Load an XML document from a file.
     *
     * @param file The XML file to load
     * @param validate Whether to validate the XML document
     * @return The parsed Document
     * @throws IOException if the file cannot be read
     */
    @Throws(IOException::class)
    fun loadDocument(file: File, validate: Boolean): Document {
        val factory = DocumentBuilderFactory.newInstance()
        factory.isNamespaceAware = true
        factory.isValidating = validate

        val builder = factory.newDocumentBuilder()
        return builder.parse(file)
    }

    /**
     * Parse an XML file into a Document (namespace-aware, non-validating).
     * Convenient shorthand for test usage.
     *
     * @param file The XML file to parse
     * @return The parsed Document
     */
    fun parseXmlFile(file: File): Document {
        val factory = DocumentBuilderFactory.newInstance()
        factory.isNamespaceAware = true
        val builder = factory.newDocumentBuilder()
        return builder.parse(file)
    }

    /**
     * Get node values using XPath expression.
     *
     * @param document The XML document to query
     * @param expression The XPath expression
     * @return List of text values, or null if no matches found
     */
    fun getNodeValues(document: Document?, expression: String): List<String>? {
        if (document == null) return null

        val nodeList = getNodeList(document, expression) ?: return null

        val values = mutableListOf<String>()
        for (i in 0 until nodeList.length) {
            val node = nodeList.item(i)
            val value = node.textContent
            if (value != null && value.isNotEmpty()) {
                values.add(value)
            }
        }

        return if (values.isEmpty()) null else values
    }

    /**
     * Get a NodeList using XPath expression.
     *
     * @param document The XML document to query
     * @param expression The XPath expression
     * @return NodeList of matching nodes, or null if no matches found
     */
    fun getNodeList(document: Document?, expression: String): NodeList? {
        if (document == null) return null

        val xPathFactory = XPathFactory.newInstance()
        val xpath = xPathFactory.newXPath()

        // Set namespace context if document has namespaces
        xpath.namespaceContext = PersonalNamespaceContext(document)

        val result = xpath.evaluate(expression, document, XPathConstants.NODESET)
        return result as? NodeList
    }

    /**
     * Evaluate an XPath expression and return the result as a string.
     * Uses a custom namespace context for common namespaces (JHOVE, METS, MIX, etc.).
     *
     * @param doc The XML document to query
     * @param expression The XPath expression
     * @param namespaceContext Optional custom namespace context. If null, uses CommonNamespaceContext
     * @return The result as a string, or null if no match
     */
    fun xpath(
        doc: Document,
        expression: String,
        namespaceContext: javax.xml.namespace.NamespaceContext? = null
    ): String? {
        val xPath = XPathFactory.newInstance().newXPath()
        xPath.namespaceContext = namespaceContext ?: CommonNamespaceContext()
        return xPath.evaluate(expression, doc)?.takeIf { it.isNotEmpty() }
    }

    /**
     * Evaluate an XPath expression and return the result as a NodeList.
     * Uses a custom namespace context for common namespaces (JHOVE, METS, MIX, etc.).
     *
     * @param doc The XML document to query
     * @param expression The XPath expression
     * @param namespaceContext Optional custom namespace context. If null, uses CommonNamespaceContext
     * @return The result as a NodeList
     */
    fun xpathNodeList(
        doc: Document,
        expression: String,
        namespaceContext: javax.xml.namespace.NamespaceContext? = null
    ): NodeList {
        val xPath = XPathFactory.newInstance().newXPath()
        xPath.namespaceContext = namespaceContext ?: CommonNamespaceContext()
        return xPath.evaluate(expression, doc, XPathConstants.NODESET) as NodeList
    }

    /**
     * Common namespace context for JHOVE, METS, MIX, and other frequently used namespaces.
     * Uses centralized namespace constants from XmlConstants.
     */
    class CommonNamespaceContext : javax.xml.namespace.NamespaceContext {
        override fun getNamespaceURI(prefix: String): String {
            return when (prefix) {
                "jhove" -> XmlConstants.JHOVE
                "mets" -> XmlConstants.METS
                "mix" -> XmlConstants.MIX
                "xlink" -> XmlConstants.XLINK
                "alto" -> XmlConstants.ALTO
                else -> javax.xml.XMLConstants.NULL_NS_URI
            }
        }

        override fun getPrefix(namespaceUri: String): String? = null

        override fun getPrefixes(namespaceUri: String): MutableIterator<String>? = null
    }
}
