package no.nb.nifi.tekst.mets

import no.nb.nifi.tekst.util.XmlConstants
import org.w3c.dom.Document
import org.w3c.dom.Element
import javax.xml.parsers.DocumentBuilderFactory

/**
 * Simple XML serializer for METS-browsing documents.
 * Uses standard Java XML APIs - no external dependencies needed.
 * Uses centralized namespace constants from XmlConstants.
 */
object MetsBrowsingSerializer {
    fun serialize(mets: Mets, mixVersion: MixVersion = MixVersion.MIX_2_0): String {
        val docBuilder = DocumentBuilderFactory.newInstance().apply {
            isNamespaceAware = true
        }.newDocumentBuilder()

        val doc = docBuilder.newDocument()

        // Create root element with all namespaces
        val root = doc.createElementNS(XmlConstants.METS, "mets:mets")
        root.setAttributeNS(XmlConstants.XMLNS, "xmlns:mets", XmlConstants.METS)
        root.setAttributeNS(XmlConstants.XMLNS, "xmlns:xlink", XmlConstants.XLINK)
        root.setAttributeNS(XmlConstants.XMLNS, "xmlns:xsi", XmlConstants.XSI)

        // Set MIX namespace based on version
        val (mixNs, mixXsd) = when (mixVersion) {
            MixVersion.MIX_1_0 -> Pair(XmlConstants.MIX, XmlConstants.MIX_XSD)
            MixVersion.MIX_2_0 -> Pair(XmlConstants.MIX2, XmlConstants.MIX2_XSD)
        }
        root.setAttributeNS(XmlConstants.XMLNS, "xmlns:mix", mixNs)
        root.setAttributeNS(XmlConstants.XSI, "xsi:schemaLocation",
            "${XmlConstants.METS} ${XmlConstants.METS_XSD} $mixNs $mixXsd")
        root.setAttribute("OBJID", mets.objId)
        doc.appendChild(root)

        // Build the document
        root.appendChild(createMetsHdr(doc, mets.metsHdr))
        mets.amdSec.forEach { root.appendChild(createAmdSec(doc, it, mixNs, mixVersion)) }
        root.appendChild(createFileSec(doc, mets.fileSec))
        root.appendChild(createStructMap(doc, mets.structMap))

        return documentToString(doc)
    }

    private fun createMetsHdr(doc: Document, hdr: MetsHdr): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:metsHdr").apply {
            setAttribute("CREATEDATE", hdr.createDate)
            setAttribute("LASTMODDATE", hdr.lastModDate)
            appendChild(createAgent(doc, hdr.agent))
        }
    }

    private fun createAgent(doc: Document, agent: Agent): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:agent").apply {
            setAttribute("ROLE", agent.role)
            setAttribute("TYPE", agent.type)
            setAttribute("OTHERTYPE", agent.otherType)
            appendChild(doc.createElementNS(XmlConstants.METS, "mets:name").apply {
                textContent = agent.name
            })
        }
    }

    private fun createAmdSec(doc: Document, amdSec: AmdSec, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:amdSec").apply {
            setAttribute("ID", amdSec.id)
            appendChild(createTechMD(doc, amdSec.techMD, mixNs, mixVersion))
        }
    }

    private fun createTechMD(doc: Document, techMD: TechMD, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:techMD").apply {
            setAttribute("ID", techMD.id)
            appendChild(createMdWrap(doc, techMD.mdWrap, mixNs, mixVersion))
        }
    }

    private fun createMdWrap(doc: Document, mdWrap: MdWrap, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:mdWrap").apply {
            setAttribute("MDTYPE", mdWrap.mdType)
            appendChild(createXmlData(doc, mdWrap.xmlData, mixNs, mixVersion))
        }
    }

    private fun createXmlData(doc: Document, xmlData: XmlData, mixNs: String, mixVersion: MixVersion): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:xmlData").apply {
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
        return doc.createElementNS(XmlConstants.METS, "mets:fileSec").apply {
            appendChild(createFileGrp(doc, fileSec.fileGrp))
        }
    }

    private fun createFileGrp(doc: Document, fileGrp: FileGrp): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:fileGrp").apply {
            setAttribute("ID", fileGrp.id)
            fileGrp.file.forEach { appendChild(createFileEntry(doc, it)) }
        }
    }

    private fun createFileEntry(doc: Document, file: FileEntry): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:file").apply {
            setAttribute("ID", file.id)
            file.seq?.let { setAttribute("SEQ", it.toString()) }
            file.admId?.let { setAttribute("ADMID", it) }
            file.use?.let { setAttribute("USE", it) }
            file.mimeType?.let { setAttribute("MIMETYPE", it) }
            file.size?.let { setAttribute("SIZE", it.toString()) }
            file.checksum?.let { setAttribute("CHECKSUM", it) }
            file.checksumType?.let { setAttribute("CHECKSUMTYPE", it) }

            file.fLocat?.forEach { appendChild(createFileLocation(doc, it)) }
            file.file?.forEach { appendChild(createFileEntry(doc, it)) }
        }
    }

    private fun createFileLocation(doc: Document, fLocat: FileLocation): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:FLocat").apply {
            setAttribute("LOCTYPE", fLocat.locType)
            setAttributeNS(XmlConstants.XLINK, "xlink:type", fLocat.xlinkType)
            setAttributeNS(XmlConstants.XLINK, "xlink:href", fLocat.xlinkHref)
        }
    }

    private fun createStructMap(doc: Document, structMap: StructMap): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:structMap").apply {
            setAttribute("TYPE", structMap.type)
            appendChild(createDiv(doc, structMap.div))
        }
    }

    private fun createDiv(doc: Document, div: Div): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:div").apply {
            div.id?.let { setAttribute("ID", it) }
            setAttribute("TYPE", div.type)
            div.order?.let { setAttribute("ORDER", it.toString()) }
            div.orderLabel?.let { setAttribute("ORDERLABEL", it) }

            div.div?.forEach { appendChild(createDiv(doc, it)) }
            div.fptr?.let { appendChild(createFilePointer(doc, it)) }
            div.fptrSingle?.let {
                appendChild(doc.createElementNS(XmlConstants.METS, "mets:fptr").apply {
                    setAttribute("FILEID", it)
                })
            }
        }
    }

    private fun createFilePointer(doc: Document, fptr: FilePointer): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:fptr").apply {
            appendChild(createPar(doc, fptr.par))
        }
    }

    private fun createPar(doc: Document, par: Par): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:par").apply {
            par.area.forEach { appendChild(createArea(doc, it)) }
        }
    }

    private fun createArea(doc: Document, area: Area): Element {
        return doc.createElementNS(XmlConstants.METS, "mets:area").apply {
            setAttribute("FILEID", area.fileId)
        }
    }

    private fun documentToString(doc: Document): String {
        val result = StringBuilder()

        // XML declaration with single quotes to match expected format
        result.append("<?xml version='1.0' encoding='UTF-8'?>\n")

        // Serialize root element
        serializeElement(doc.documentElement, result, 0)

        // Remove trailing newline to match expected format
        return result.toString().trimEnd('\n')
    }

    /**
     * Custom XML serializer for exact formatting control.
     */
    private fun serializeElement(element: Element, result: StringBuilder, indentLevel: Int) {
        val indent = "  ".repeat(indentLevel)

        // Start tag
        result.append(indent).append("<").append(element.nodeName)

        // Serialize attributes in a specific order
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
            // Self-closing tag
            result.append("/>\n")
        } else if (hasTextContent && !hasElementChildren) {
            // Text-only element - inline
            result.append(">").append(textContent).append("</").append(element.nodeName).append(">\n")
        } else {
            // Has child elements
            result.append(">\n")

            // Serialize children
            for (i in 0 until children.length) {
                val child = children.item(i)
                if (child is Element) {
                    serializeElement(child, result, indentLevel + 1)
                }
            }

            // Close tag
            result.append(indent).append("</").append(element.nodeName).append(">\n")
        }
    }

    /**
     * Serialize attributes in the correct order matching expected format.
     */
    private fun serializeAttributes(element: Element, result: StringBuilder) {
        val attrs = mutableMapOf<String, String>()

        // Collect all attributes
        for (i in 0 until element.attributes.length) {
            val attr = element.attributes.item(i)
            // Skip xmlns declarations - they're handled separately for root
            if (!attr.nodeName.startsWith("xmlns")) {
                attrs[attr.nodeName] = attr.nodeValue
            }
        }

        // Special handling for root element to ensure correct namespace order
        if (element.localName == "mets" && element.namespaceURI == XmlConstants.METS) {
            result.append(" xmlns:xsi=\"${XmlConstants.XSI}\"")
            result.append(" xmlns:mix=\"${XmlConstants.MIX}\"")
            result.append(" xmlns:mets=\"${XmlConstants.METS}\"")
            result.append(" xmlns:xlink=\"${XmlConstants.XLINK}\"")
        }

        // Define attribute order - specific attributes first, matching expected file order exactly
        // Special case for div elements: ID comes before TYPE
        val isDiv = element.localName == "div"

        val orderedKeys = if (isDiv) {
            listOf(
                "ID", "ORDER", "ORDERLABEL", "TYPE", "FILEID"
            )
        } else {
            listOf(
                // Root element attributes
                "OBJID", "xsi:schemaLocation",
                // Header attributes
                "CREATEDATE", "LASTMODDATE",
                // Agent attributes
                "ROLE", "TYPE", "OTHERTYPE",
                // Section attributes
                "ID", "SEQ",
                // Metadata attributes
                "MDTYPE",
                // File attributes - specific order from expected file
                "ADMID", "USE", "MIMETYPE", "SIZE", "CHECKSUM", "CHECKSUMTYPE",
                // Location attributes
                "LOCTYPE", "xlink:type", "xlink:href",
                // Structure attributes
                "ORDER", "ORDERLABEL", "FILEID"
            )
        }

        // Write attributes in order
        for (key in orderedKeys) {
            attrs[key]?.let { value ->
                result.append(" ").append(key).append("=\"").append(value).append("\"")
                attrs.remove(key)
            }
        }

        // Write any remaining attributes alphabetically
        attrs.keys.sorted().forEach { key ->
            result.append(" ").append(key).append("=\"").append(attrs[key]!!).append("\"")
        }
    }
}
