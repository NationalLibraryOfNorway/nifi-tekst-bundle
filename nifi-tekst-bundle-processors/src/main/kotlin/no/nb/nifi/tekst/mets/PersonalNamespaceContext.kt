package no.nb.nifi.tekst.mets

import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.util.*
import javax.xml.XMLConstants
import javax.xml.namespace.NamespaceContext


class PersonalNamespaceContext(xmlNode: Node) : NamespaceContext {
    private val PREFIX_MAPPING = HashMap<String, String>()

    /**
     * Resolves namespaces from the xmlNode provided.
     *
     * @param xmlNode The node to resolve namespaces from. Handles both org.w3c.dom.Document and a org.w3c.dom.Element
     */
    init {
        val testNode: Node = if (xmlNode is Document) {
            xmlNode.documentElement
        } else {
            xmlNode
        }
        if (testNode.attributes != null) {
            retrievePrefixes(testNode)
        }
    }

    /**
     * Retrieving prefixes from the supplied node and sub-nodes
     * Storing prefixes in lowercase to make sure we handles both uppercase and lowercase prefixes
     *
     * @param testNode
     */
    private fun retrievePrefixes(testNode: Node) {
        for (counter in 0..<testNode.attributes.length) {
            val attributeNode: Node = testNode.attributes.item(counter)
            if (attributeNode.nodeName.startsWith("xmlns")) {
                val parts: List<String> = attributeNode.nodeName.split(":")
                if (parts.size == 1) {
                    // Using element name as prefix -> <mets xmlns="http://www.loc.gov/METS/" ...>
                    PREFIX_MAPPING[testNode.nodeName.lowercase()] = attributeNode.nodeValue
                } else {
                    // Using second part of attribute as prefix -> <mets xmlns:mix="http://www.loc.gov/mix/" ...>
                    PREFIX_MAPPING[parts[1].lowercase(Locale.getDefault())] = attributeNode.nodeValue
                }
            }
        }

        val childs: NodeList = testNode.childNodes
        for (i in 0..<childs.length) {
            val child: Node = childs.item(i)
            if (child.nodeType == Node.ELEMENT_NODE) {
                retrievePrefixes(child)
            }
        }
    }

    /**
     *
     * Get Namespace URI bound to a prefix in the current scope.
     *
     * @param prefix prefix to look up.
     *
     * @return Namespace URI bound to prefix in the current scope
     * @throws IllegalArgumentException When `prefix` is `null`
     */
    override fun getNamespaceURI(prefix: String): String? {
        var prefix = prefix
        prefix = prefix.lowercase(Locale.getDefault())

        if (PREFIX_MAPPING.containsKey(prefix)) {
            return PREFIX_MAPPING[prefix]
        }
        return XMLConstants.NULL_NS_URI
    }


    /**
     * This method isn't necessary for XPath processing
     *
     * @param uri The URI to get prefix for.
     *
     * @return Method not implemented always throws `UnsupportedOperationException`
     */
    override fun getPrefix(uri: String?): String {
        throw UnsupportedOperationException()
    }

    /**
     * This method isn't necessary for XPath processing
     *
     * @param uri The URI to get prefixes for.
     *
     * @return Method not implemented always throws `UnsupportedOperationException`
     */
    override fun getPrefixes(uri: String?): MutableIterator<String>? {
        throw UnsupportedOperationException()
    }
}