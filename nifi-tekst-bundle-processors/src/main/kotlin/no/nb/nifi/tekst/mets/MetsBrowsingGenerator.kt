package no.nb.nifi.tekst.mets

import java.io.File

/**
 * Clean, simple generator for METS-browsing files using direct Kotlin data classes.
 * Supports both METS v1 and METS v2 output formats.
 *
 * Example usage:
 * ```
 * val generator = MetsBrowsingGenerator()
 *
 * // Generate METS v1 (default)
 * val xmlV1 = generator.generateMetsBrowsing(
 *     objId = "pliktmonografi_000040956",
 *     pages = listOf(...)
 * )
 *
 * // Generate METS v2
 * val xmlV2 = generator.generateMetsBrowsing(
 *     objId = "pliktmonografi_000040956",
 *     pages = listOf(...),
 *     version = MetsVersion.METS_2
 * )
 * ```
 */
class MetsBrowsingGenerator {

    /**
     * Generate a METS-browsing XML document.
     *
     * @param objId The object identifier (e.g., "pliktmonografi_000040956")
     * @param pages List of page information
     * @param agentName Optional agent name (defaults to "nifi-tekst-bundle")
     * @param version METS version to generate (defaults to METS_2)
     * @param mixVersion MIX version to use for technical metadata (defaults to MIX_2_0)
     * @return XML string of the METS-browsing document
     */
    fun generateMetsBrowsing(
        objId: String,
        pages: List<PageInfo>,
        agentName: String = "nifi-tekst-bundle",
        version: MetsVersion = MetsVersion.METS_2,
        mixVersion: MixVersion = MixVersion.MIX_2_0
    ): String {
        val builder = MetsBrowsingBuilder(objId)
            .withAgentName(agentName)

        pages.forEach { page ->
            builder.addPage(
                pageNumber = page.pageNumber,
                altoFile = page.altoFile,
                imageFile = page.imageFile,
                resolution = page.resolution
            )
        }

        val mets = builder.build()

        return when (version) {
            MetsVersion.METS_1 -> MetsBrowsingSerializer.serialize(mets, mixVersion)
            MetsVersion.METS_2 -> Mets2BrowsingSerializer.serialize(mets, mixVersion)
        }
    }

    /**
     * Save METS-browsing XML to a file.
     *
     * @param objId The object identifier
     * @param pages List of page information
     * @param outputFile Output file path
     * @param agentName Optional agent name
     * @param version METS version to generate (defaults to METS_2)
     * @param mixVersion MIX version to use for technical metadata (defaults to MIX_2_0)
     */
    fun saveMetsBrowsing(
        objId: String,
        pages: List<PageInfo>,
        outputFile: File,
        agentName: String = "nifi-tekst-bundle",
        version: MetsVersion = MetsVersion.METS_2,
        mixVersion: MixVersion = MixVersion.MIX_2_0
    ) {
        val xml = generateMetsBrowsing(objId, pages, agentName, version, mixVersion)
        outputFile.writeText(xml)
    }
}