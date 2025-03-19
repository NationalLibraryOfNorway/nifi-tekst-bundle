package no.nb.nifi.tekst.mets

import no.nb.nifi.tekst.exception.MetsCreateException
import org.w3c.dom.DOMException
import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import org.xml.sax.SAXException
import java.io.*
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.parsers.ParserConfigurationException
import javax.xml.transform.TransformerException
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathExpressionException
import javax.xml.xpath.XPathFactory

object XmlHelper {
    fun getXmlParameterValue(node: Node, valueName: String?): String? {
        return getXmlParameterValue(node, valueName, null)
    }

    @Synchronized
    fun getXmlParameterValue(node: Node, valueName: String?, defVal: String?): String? {
        val attr = node.attributes.getNamedItem(valueName)
        if (attr != null) {
            val retVal = attr.nodeValue
            return retVal
        } else {
            return defVal
        }
    }

    @Synchronized
    fun getXmlNodeValue(node: Node, valueName: String?, defVal: String): String {
        var retval: String? = null

        var child = node.firstChild
        while (child != null) {
            if (child.nodeName.equals(valueName, ignoreCase = true)) {
                retval = child.textContent
            }
            child = child.nextSibling
        }

        return retval ?: defVal
    }

    @Synchronized
    fun getXmlNodeValue(node: Node, valueName: String?): Node? {
        var retval: Node? = null

        var child = node.firstChild
        while (child != null) {
            if (child.nodeName.equals(valueName, ignoreCase = true)) {
                retval = child
                break
            }
            child = child.nextSibling
        }

        return retval
    }

    fun getNodeValues(xmlNode: Node?, xPathExpr: String?): Array<String?>? {
        var retVal: Array<String?>? = null

        try {
            val nList = getNodeList(xmlNode, xPathExpr) ?: return null

            if (nList.length > 0) {
                retVal = arrayOfNulls(nList.length)
            }

            for (pos in 0..<nList.length) {
                retVal!![pos] = nList.item(pos).textContent
            }
        } catch (var5: DOMException) {
            retVal = null
        }

        return retVal
    }

    fun getNodeList(xmlNode: Node?, xPathExpr: String?): NodeList? {
        val factory = XPathFactory.newInstance()
        val xPath = factory.newXPath()
        xPath.namespaceContext = xmlNode?.let { PersonalNamespaceContext(it) }

        var retVal: NodeList?
        try {
            val expr = xPath.compile(xPathExpr)
            retVal = expr.evaluate(xmlNode, XPathConstants.NODESET) as NodeList
        } catch (var6: XPathExpressionException) {
            retVal = null
        }

        return retVal
    }

    fun getStringValue(doc: Document?, xPathExpr: String?): String? {
        var retVal: String? = null
        val factory = XPathFactory.newInstance()
        val xPath = factory.newXPath()
        xPath.namespaceContext = PersonalNamespaceContext(doc)

        try {
            val expr = xPath.compile(xPathExpr)
            retVal = expr.evaluate(doc, XPathConstants.STRING) as String
        } catch (var6: XPathExpressionException) {
        }

        return retVal
    }

    @Throws(TransformerException::class)
    fun writeNodeToFile(node: Node?, fil: File) {
        val output = StreamResult(fil)
        writeNodeToStreamResult(node, output)
    }

    @Throws(TransformerException::class)
    fun writeNodeToStreamResult(node: Node?, output: StreamResult?) {
        val source = DOMSource(node)
        val tFac = TransformerFactory.newInstance()
        val transformer = tFac.newTransformer()
        val xp = XPathFactory.newInstance().newXPath()

        try {
            val nl = xp.evaluate("//text()[normalize-space(.)='']", node, XPathConstants.NODESET) as NodeList

            for (i in 0..<nl.length) {
                val n2 = nl.item(i)
                n2.parentNode.removeChild(n2)
            }
        } catch (ex: XPathExpressionException) {
            throw TransformerException("Unable to write stream result: " + (ex as Exception).localizedMessage, ex)
        } catch (ex: DOMException) {
            throw TransformerException("Unable to write stream result: " + (ex as Exception).localizedMessage, ex)
        }

        transformer.setOutputProperty("encoding", "UTF-8")
        transformer.setOutputProperty("method", "xml")
        transformer.setOutputProperty("indent", "yes")
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2")
        transformer.transform(source, output)
    }

    @JvmOverloads
    @Throws(IOException::class)
    fun loadDocument(xmlDocument: Any, NamespaceAware: Boolean = true): Document {
        val dbfactory = DocumentBuilderFactory.newInstance()
        dbfactory.isNamespaceAware = NamespaceAware

        try {
            val builder = dbfactory.newDocumentBuilder()
            val retVal: Document
            if (xmlDocument is File) {
                retVal = builder.parse(xmlDocument)
            } else if (xmlDocument is InputStream) {
                retVal = builder.parse(xmlDocument)
            } else if (xmlDocument is String) {
                val iSource = InputSource(StringReader(xmlDocument))
                retVal = builder.parse(iSource)
            } else {
                if (xmlDocument !is ByteArray) {
                    throw IOException("Unable to parse document of type '" + xmlDocument.javaClass.canonicalName + "'")
                }

                val iSource = InputSource(ByteArrayInputStream(xmlDocument))
                retVal = builder.parse(iSource)
            }

            return retVal
        } catch (ex: SAXException) {
            throw IOException("Error parsing document, MSG: " + (ex as Exception).localizedMessage, ex)
        } catch (ex: ParserConfigurationException) {
            throw IOException("Error parsing document, MSG: " + (ex as Exception).localizedMessage, ex)
        }
    }

    @Throws(MetsCreateException::class)
    private fun getObjectFromFile(file: File, xpath: String): Any {
        try {
            val xmlNode: Node = loadNodeFromFile(file, xpath).item(0)

            val metaUtil: MetadataUtils = MetadataUtils.getInstance()
            metaUtil.setValidating(false)
            return metaUtil.createUnmarshaller().unmarshal(xmlNode)
        } catch (ex: javax.xml.bind.JAXBException) {
            throw MetsCreateException("Feil under unmarshalling fra xml til objekt: " + file.absolutePath, ex)
        }
    }

    @Throws(MetsCreateException::class)
    private fun loadNodeFromFile(file: File, xPath: String): NodeList {
        val document: Document
        try {
            document = loadDocument(file, true)
        } catch (ex: IOException) {
            throw MetsCreateException("Klarte ikke å åpne metadatafilen: " + file.absolutePath, ex)
        }
        val nodeList = getNodeList(document, xPath)
        if (nodeList?.item(0) == null) {
            throw MetsCreateException("Finner ikke forventet data i metadatafilen: " + file.absolutePath + ", xpath: " + xPath)
        }
        return nodeList
    }

    private fun getFileFromPattern(dir: File, regExp: Regex, allowNoFileFound: Boolean): File? {
        val filter = FilenameFilter { _, name -> name.matches(regExp) }
        val filesFound: Array<File> = dir.listFiles(filter)

        return if (filesFound.size == 1) {
            filesFound[0]
        } else if (filesFound.isEmpty() && !allowNoFileFound) {
            throw MetsCreateException("No file found with filename: " + regExp + " in dir: " + dir.absolutePath)
        } else if (filesFound.size > 1) {
            throw MetsCreateException("Found more than one file with filename: $regExp")
        } else {
            null
        }
    }
}
