package no.nb.nifi.tekst.mets

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * Builder for creating METS-browsing documents with a clean, fluent API.
 *
 * Example usage:
 * ```
 * val mets = MetsBrowsingBuilder("pliktmonografi_000040956")
 *     .addPage(
 *         pageNumber = 1,
 *         altoFile = AltoFileInfo(...),
 *         imageFile = ImageFileInfo(...),
 *         resolution = ImageResolution(...)
 *     )
 *     .build()
 * ```
 */
class MetsBrowsingBuilder(private val objId: String) {
    private val pages = mutableListOf<PageInfo>()
    private var agentName: String = "nifi-tekst-bundle"

    fun withAgentName(name: String) = apply {
        this.agentName = name
    }

    fun addPage(
        pageNumber: Int,
        altoFile: AltoFileInfo,
        imageFile: ImageFileInfo,
        resolution: ImageResolution
    ) = apply {
        pages.add(PageInfo(pageNumber, altoFile, imageFile, resolution))
    }

    fun build(): Mets {
        val timestamp = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

        return Mets(
            objId = objId,
            metsHdr = createMetsHeader(timestamp),
            amdSec = createAmdSections(),
            fileSec = createFileSec(),
            structMap = createStructMap()
        )
    }

    private fun createMetsHeader(timestamp: String): MetsHdr {
        return MetsHdr(
            createDate = timestamp,
            lastModDate = timestamp,
            agent = Agent(name = agentName)
        )
    }

    private fun createAmdSections(): List<AmdSec> {
        val sections = mutableListOf<AmdSec>()

        // Add JPEG2000 encoding section (only once)
        sections.add(createJpeg2000EncodingSection())

        // Add resolution section for each page
        pages.forEach { page ->
            sections.add(createResolutionSection(page))
        }

        return sections
    }

    private fun createJpeg2000EncodingSection(): AmdSec {
        return AmdSec(
            id = "JPEG2000_ENCODING",
            techMD = TechMD(
                id = "TM_J2K",
                mdWrap = MdWrap(
                    xmlData = XmlData(
                        mix = Mix(
                            basicImageInformation = BasicImageInformation(
                                specialFormatCharacteristics = SpecialFormatCharacteristics(
                                    jpeg2000 = Jpeg2000(
                                        encodingOptions = EncodingOptions()
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    }

    private fun createResolutionSection(page: PageInfo): AmdSec {
        return AmdSec(
            id = "RESOLUTION_P${page.pageNumber.toString().padStart(4, '0')}",
            techMD = TechMD(
                id = "TM_${page.pageNumber.toString().padStart(4, '0')}",
                mdWrap = MdWrap(
                    xmlData = XmlData(
                        mix = Mix(
                            basicImageInformation = BasicImageInformation(
                                basicImageCharacteristics = BasicImageCharacteristics(
                                    imageWidth = page.resolution.width,
                                    imageHeight = page.resolution.height
                                )
                            ),
                            imageAssessmentMetadata = ImageAssessmentMetadata(
                                spatialMetrics = SpatialMetrics(
                                    samplingFrequencyUnit = 3,
                                    xSamplingFrequency = page.resolution.xSamplingFrequency,
                                    ySamplingFrequency = page.resolution.ySamplingFrequency
                                )
                            )
                        )
                    )
                )
            )
        )
    }

    private fun createFileSec(): FileSec {
        return FileSec(
            fileGrp = FileGrp(
                id = "BROWSINGGRP",
                file = pages.map { page -> createFileEntry(page) }
            )
        )
    }

    private fun createFileEntry(page: PageInfo): FileEntry {
        val pageNumberStr = page.pageNumber.toString().padStart(4, '0')

        return FileEntry(
            id = "DO_${pageNumberStr}",
            seq = page.pageNumber,
            file = listOf(
                // ALTO file entry (layout)
                FileEntry(
                    id = "ALTO_${pageNumberStr}",
                    use = "layout",
                    mimeType = "application/xml",
                    size = page.altoFile.size,
                    checksum = page.altoFile.checksum,
                    checksumType = "MD5",
                    fLocat = listOf(
                        FileLocation(
                            locType = "URN",
                            xlinkHref = page.altoFile.urn
                        )
                    )
                ),
                // Browsing copy
                FileEntry(
                    id = "DO_${pageNumberStr}_003",
                    admId = "JPEG2000_ENCODING RESOLUTION_P${pageNumberStr}",
                    use = "browsing",
                    mimeType = "image/jp2",
                    size = page.imageFile.size,
                    checksum = page.imageFile.checksum,
                    checksumType = "MD5",
                    fLocat = listOf(
                        FileLocation(
                            locType = "URN",
                            xlinkHref = page.imageFile.urn
                        )
                    )
                )
            )
        )
    }

    private fun createStructMap(): StructMap {
        return StructMap(
            div = Div(
                type = "PAGES",
                div = pages.map { page -> createPageDiv(page) }
            )
        )
    }

    private fun createPageDiv(page: PageInfo): Div {
        val pageNumberStr = page.pageNumber.toString().padStart(4, '0')

        return Div(
            id = "DIV${page.pageNumber}",
            order = page.pageNumber,
            orderLabel = page.pageNumber.toString(),
            type = "PAGE",
            fptr = FilePointer(
                par = Par(
                    area = listOf(
                        Area(fileId = "ALTO_${pageNumberStr}")
                    )
                )
            ),
            fptrSingle = "DO_${pageNumberStr}_003"
        )
    }
}

data class PageInfo(
    val pageNumber: Int,
    val altoFile: AltoFileInfo,
    val imageFile: ImageFileInfo,
    val resolution: ImageResolution
)

data class AltoFileInfo(
    val size: Long,
    val checksum: String,
    val urn: String,
    val url: String
)

data class ImageFileInfo(
    val size: Long,
    val checksum: String,
    val urn: String,
    val url: String
)

data class ImageResolution(
    val width: Int,
    val height: Int,
    val xSamplingFrequency: SamplingFrequency = SamplingFrequency(4000000, 25400),
    val ySamplingFrequency: SamplingFrequency = SamplingFrequency(4000000, 25400)
)
