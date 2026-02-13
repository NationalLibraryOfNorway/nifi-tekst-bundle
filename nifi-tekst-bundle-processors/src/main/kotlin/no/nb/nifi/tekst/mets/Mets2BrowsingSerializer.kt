package no.nb.nifi.tekst.mets

import no.nb.nifi.tekst.util.XmlConstants
import org.w3c.dom.Document
import org.w3c.dom.Element
import javax.xml.parsers.DocumentBuilderFactory

/**
 * XML serializer for METS 2 browsing documents.
 * Generates METS version 2 compliant XML output.
 *
 * Key differences from METS v1:
 * - Namespace: http://www.loc.gov/METS/v2
 * - amdSec/techMD → mdSec with md elements
 * - ADMID → MDID attribute for referencing metadata
 * - Agent simplified (no OTHERTYPE attribute)
 * - structMap → inside structSec element
 * - FLocat uses LOCREF instead of xlink:href
 */
object Mets2BrowsingSerializer {

    fun serialize(mets: Mets, mixVersion: MixVersion = MixVersion.MIX_2_0): String {
        val docBuilder = DocumentBuilderFactory.newInstance().apply {
            isNamespaceAware = true
        }.newDocumentBuilder()

        val doc = docBuilder.newDocument()

        // Create root element with METS 2 namespace
        val root = doc.createElementNS(XmlConstants.METS2, "mets:mets")
        root.setAttributeNS(XmlConstants.XMLNS, "xmlns:mets", XmlConstants.METS2)
        root.setAttributeNS(XmlConstants.XMLNS, "xmlns:xsi", XmlConstants.XSI)

        // Set MIX namespace based on version
        val (mixNs, mixXsd) = when (mixVersion) {
            MixVersion.MIX_1_0 -> Pair(XmlConstants.MIX, XmlConstants.MIX_XSD)
            MixVersion.MIX_2_0 -> Pair(XmlConstants.MIX2, XmlConstants.MIX2_XSD)
        }
        root.setAttributeNS(XmlConstants.XMLNS, "xmlns:mix", mixNs)
        root.setAttributeNS(XmlConstants.XSI, "xsi:schemaLocation",
            "${XmlConstants.METS2} ${XmlConstants.METS2_XSD} $mixNs $mixXsd")
        root.setAttribute("OBJID", mets.objId)
        doc.appendChild(root)

        // Build the document
        root.appendChild(createMetsHdr(doc, mets.metsHdr))
        root.appendChild(createMdSec(doc, mets.amdSec, mixNs, mixVersion))
        root.appendChild(createFileSec(doc, mets.fileSec))
        root.appendChild(createStructSec(doc, mets.structMap))

        return documentToString(doc)
    }

    private fun createMetsHdr(doc: Document, hdr: MetsHdr): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:metsHdr").apply {
            setAttribute("CREATEDATE", hdr.createDate)
            setAttribute("LASTMODDATE", hdr.lastModDate)
            appendChild(createAgent(doc, hdr.agent))
        }
    }

    private fun createAgent(doc: Document, agent: Agent): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:agent").apply {
            setAttribute("ROLE", agent.role)
            // METS 2 uses TYPE directly without OTHERTYPE
            setAttribute("TYPE", if (agent.type == "OTHER") agent.otherType else agent.type)
            appendChild(doc.createElementNS(XmlConstants.METS2, "mets:name").apply {
                textContent = agent.name
            })
        }
    }

    /**
     * METS 2 uses mdSec with md elements instead of multiple amdSec elements
     */
    private fun createMdSec(doc: Document, amdSecs: List<AmdSec>, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:mdSec").apply {
            amdSecs.forEach { amdSec ->
                appendChild(createMd(doc, amdSec, mixNs, mixVersion))
            }
        }
    }

    private fun createMd(doc: Document, amdSec: AmdSec, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:md").apply {
            setAttribute("ID", amdSec.id)
            setAttribute("USE", "TECHNICAL")
            appendChild(createMdWrap(doc, amdSec.techMD.mdWrap, mixNs, mixVersion))
        }
    }

    private fun createMdWrap(doc: Document, mdWrap: MdWrap, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:mdWrap").apply {
            setAttribute("MDTYPE", mdWrap.mdType)
            appendChild(createXmlData(doc, mdWrap.xmlData, mixNs, mixVersion))
        }
    }

    private fun createXmlData(doc: Document, xmlData: XmlData, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:xmlData").apply {
            appendChild(createMix(doc, xmlData.mix, mixNs, mixVersion))
        }
    }

    private fun createMix(doc: Document, mix: Mix, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(mixNs, "mix:mix").apply {
            mix.basicImageInformation?.let {
                appendChild(createBasicImageInformation(doc, it, mixNs, mixVersion))
            }
            mix.imageAssessmentMetadata?.let {
                appendChild(createImageAssessmentMetadata(doc, it, mixNs, mixVersion))
            }
        }
    }

    private fun createBasicImageInformation(doc: Document, bii: BasicImageInformation, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(mixNs, "mix:BasicImageInformation").apply {
            bii.specialFormatCharacteristics?.let {
                appendChild(createSpecialFormatCharacteristics(doc, it, mixNs, mixVersion))
            }
            bii.basicImageCharacteristics?.let {
                appendChild(createBasicImageCharacteristics(doc, it, mixNs, mixVersion))
            }
        }
    }

    private fun createSpecialFormatCharacteristics(doc: Document, sfc: SpecialFormatCharacteristics, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(mixNs, "mix:SpecialFormatCharacteristics").apply {
            appendChild(createJpeg2000(doc, sfc.jpeg2000, mixNs, mixVersion))
        }
    }

    private fun createJpeg2000(doc: Document, jpeg: Jpeg2000, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(mixNs, "mix:JPEG2000").apply {
            appendChild(createEncodingOptions(doc, jpeg.encodingOptions, mixNs, mixVersion))
        }
    }

    private fun createEncodingOptions(doc: Document, eo: EncodingOptions, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(mixNs, "mix:EncodingOptions").apply {
            // MIX v2 uses Tiles container with tileWidth and tileHeight
            // MIX v1 uses simple tiles string
            when (mixVersion) {
                MixVersion.MIX_1_0 -> {
                    appendChild(doc.createElementNS(mixNs, "mix:tiles").apply {
                        textContent = eo.tiles
                    })
                }
                MixVersion.MIX_2_0 -> {
                    val (width, height) = eo.getTileDimensions()
                    appendChild(doc.createElementNS(mixNs, "mix:Tiles").apply {
                        appendChild(doc.createElementNS(mixNs, "mix:tileWidth").apply {
                            textContent = width.toString()
                        })
                        appendChild(doc.createElementNS(mixNs, "mix:tileHeight").apply {
                            textContent = height.toString()
                        })
                    })
                }
            }
            appendChild(doc.createElementNS(mixNs, "mix:qualityLayers").apply {
                textContent = eo.qualityLayers.toString()
            })
            appendChild(doc.createElementNS(mixNs, "mix:resolutionLevels").apply {
                textContent = eo.resolutionLevels.toString()
            })
        }
    }

    private fun createBasicImageCharacteristics(doc: Document, bic: BasicImageCharacteristics, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(mixNs, "mix:BasicImageCharacteristics").apply {
            appendChild(doc.createElementNS(mixNs, "mix:imageWidth").apply {
                textContent = bic.imageWidth.toString()
            })
            appendChild(doc.createElementNS(mixNs, "mix:imageHeight").apply {
                textContent = bic.imageHeight.toString()
            })
        }
    }

    private fun createImageAssessmentMetadata(doc: Document, iam: ImageAssessmentMetadata, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(mixNs, "mix:ImageAssessmentMetadata").apply {
            appendChild(createSpatialMetrics(doc, iam.spatialMetrics, mixNs, mixVersion))
        }
    }

    private fun createSpatialMetrics(doc: Document, sm: SpatialMetrics, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(mixNs, "mix:SpatialMetrics").apply {
            // MIX v2 uses string values for samplingFrequencyUnit
            // MIX v1 uses integer values
            appendChild(doc.createElementNS(mixNs, "mix:samplingFrequencyUnit").apply {
                textContent = when (mixVersion) {
                    MixVersion.MIX_1_0 -> sm.samplingFrequencyUnit.toString()
                    MixVersion.MIX_2_0 -> sm.getSamplingFrequencyUnitString()
                }
            })
            appendChild(createSamplingFrequency(doc, "mix:xSamplingFrequency", sm.xSamplingFrequency, mixNs, mixVersion))
            appendChild(createSamplingFrequency(doc, "mix:ySamplingFrequency", sm.ySamplingFrequency, mixNs, mixVersion))
        }
    }

    private fun createSamplingFrequency(doc: Document, elementName: String, sf: SamplingFrequency, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(mixNs, elementName).apply {
            appendChild(doc.createElementNS(mixNs, "mix:numerator").apply {
                textContent = sf.numerator.toString()
            })
            appendChild(doc.createElementNS(mixNs, "mix:denominator").apply {
                textContent = sf.denominator.toString()
            })
        }
    }

    private fun createFileSec(doc: Document, fileSec: FileSec): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:fileSec").apply {
            appendChild(createFileGrp(doc, fileSec.fileGrp))
        }
    }

    private fun createFileGrp(doc: Document, fileGrp: FileGrp): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:fileGrp").apply {
            setAttribute("ID", fileGrp.id)
            setAttribute("USE", "BROWSING")
            fileGrp.file.forEach { appendChild(createFileEntry(doc, it)) }
        }
    }

    private fun createFileEntry(doc: Document, file: FileEntry): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:file").apply {
            setAttribute("ID", file.id)
            file.seq?.let { setAttribute("SEQ", it.toString()) }
            // METS 2 uses MDID instead of ADMID
            file.admId?.let { setAttribute("MDID", it) }
            file.use?.let { setAttribute("USE", it) }
            file.mimeType?.let { setAttribute("MIMETYPE", it) }
            file.size?.let { setAttribute("SIZE", it.toString()) }
            file.checksum?.let { setAttribute("CHECKSUM", it) }
            file.checksumType?.let { setAttribute("CHECKSUMTYPE", it) }

            file.fLocat?.forEach { appendChild(createFileLocation(doc, it)) }
            file.file?.forEach { appendChild(createFileEntry(doc, it)) }
        }
    }

    /**
     * METS 2 FLocat uses LOCREF attribute instead of xlink:href
     */
    private fun createFileLocation(doc: Document, fLocat: FileLocation): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:FLocat").apply {
            setAttribute("LOCTYPE", fLocat.locType)
            setAttribute("LOCREF", fLocat.xlinkHref)
        }
    }

    /**
     * METS 2 uses structSec to contain structMap elements
     */
    private fun createStructSec(doc: Document, structMap: StructMap): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:structSec").apply {
            appendChild(createStructMap(doc, structMap))
        }
    }

    private fun createStructMap(doc: Document, structMap: StructMap): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:structMap").apply {
            setAttribute("TYPE", structMap.type)
            appendChild(createDiv(doc, structMap.div))
        }
    }

    private fun createDiv(doc: Document, div: Div): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:div").apply {
            div.id?.let { setAttribute("ID", it) }
            setAttribute("TYPE", div.type)
            div.order?.let { setAttribute("ORDER", it.toString()) }
            div.orderLabel?.let { setAttribute("ORDERLABEL", it) }

            div.div?.forEach { appendChild(createDiv(doc, it)) }
            div.fptr?.let { appendChild(createFilePointer(doc, it)) }
            div.fptrSingle?.let {
                appendChild(doc.createElementNS(XmlConstants.METS2, "mets:fptr").apply {
                    setAttribute("FILEID", it)
                })
            }
        }
    }

    private fun createFilePointer(doc: Document, fptr: FilePointer): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:fptr").apply {
            appendChild(createPar(doc, fptr.par))
        }
    }

    private fun createPar(doc: Document, par: Par): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:par").apply {
            par.area.forEach { appendChild(createArea(doc, it)) }
        }
    }

    private fun createArea(doc: Document, area: Area): Element {
        return doc.createElementNS(XmlConstants.METS2, "mets:area").apply {
            setAttribute("FILEID", area.fileId)
        }
    }

    private fun documentToString(doc: Document): String {
        val result = StringBuilder()

        // XML declaration
        result.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")

        // Serialize root element
        serializeElement(doc.documentElement, result, 0)

        return result.toString().trimEnd('\n')
    }

    /**
     * Custom XML serializer for exact formatting control.
     */
    private fun serializeElement(element: Element, result: StringBuilder, indentLevel: Int) {
        val indent = "  ".repeat(indentLevel)

        // Start tag
        result.append(indent).append("<").append(element.nodeName)

        // Serialize attributes
        serializeAttributes(element, result)

        // Check if element has children
        val children = element.childNodes
        var hasElementChildren = false
        var hasTextContent = false
        val textContent = StringBuilder()

        for (i in 0 until children.length) {
            val child = children.item(i)
            when (child.nodeType) {
                org.w3c.dom.Node.ELEMENT_NODE -> hasElementChildren = true
                org.w3c.dom.Node.TEXT_NODE -> {
                    val text = child.nodeValue?.trim() ?: ""
                    if (text.isNotEmpty()) {
                        hasTextContent = true
                        textContent.append(text)
                    }
                }
            }
        }

        if (!hasElementChildren && !hasTextContent) {
            result.append("/>\n")
        } else if (hasTextContent && !hasElementChildren) {
            result.append(">").append(escapeXml(textContent.toString()))
            result.append("</").append(element.nodeName).append(">\n")
        } else {
            result.append(">\n")
            for (i in 0 until children.length) {
                val child = children.item(i)
                if (child.nodeType == org.w3c.dom.Node.ELEMENT_NODE) {
                    serializeElement(child as Element, result, indentLevel + 1)
                }
            }
            result.append(indent).append("</").append(element.nodeName).append(">\n")
        }
    }

    private fun serializeAttributes(element: Element, result: StringBuilder) {
        val attrs = element.attributes

        // Collect and sort attributes for consistent output
        val attrList = mutableListOf<Pair<String, String>>()
        for (i in 0 until attrs.length) {
            val attr = attrs.item(i)
            attrList.add(attr.nodeName to attr.nodeValue)
        }

        // Sort: xmlns first, then xsi:schemaLocation, then alphabetically
        attrList.sortWith { a, b ->
            when {
                a.first.startsWith("xmlns") && !b.first.startsWith("xmlns") -> -1
                !a.first.startsWith("xmlns") && b.first.startsWith("xmlns") -> 1
                a.first == "xsi:schemaLocation" -> 1
                b.first == "xsi:schemaLocation" -> -1
                else -> a.first.compareTo(b.first)
            }
        }

        for ((name, value) in attrList) {
            result.append(" ").append(name).append("=\"").append(escapeXml(value)).append("\"")
        }
    }

    private fun escapeXml(text: String): String {
        return text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&apos;")
    }
}

