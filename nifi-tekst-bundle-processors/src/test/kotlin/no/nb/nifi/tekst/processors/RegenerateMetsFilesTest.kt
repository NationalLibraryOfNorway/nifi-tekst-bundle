package no.nb.nifi.tekst.processors

import no.nb.nifi.tekst.mets.*
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Paths
import java.security.MessageDigest
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathFactory

/**
 * Test to regenerate expected METS files with METS v2 + MIX v2.0.
 * Run this test to update the expected test files.
 */
class RegenerateMetsFilesTest {

    private val projectFolder = Paths.get("").toAbsolutePath().toString()
    private val tekstUuid = "tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72"
    private val tekstFolder = "$projectFolder/src/test/resources/$tekstUuid"
    private val altoFolder = "$tekstFolder/access/metadata/other/ocr"
    private val imageFolder = "$tekstFolder/access/data"
    private val jhoveFolder = "$tekstFolder/access/metadata/technical/jhove"

    @Test
    @Disabled("Run manually to regenerate METS2 file")
    fun `regenerate METS2_BROWSING with MIX v2_0`() {
        val altoFolderFile = File(altoFolder)
        val imageFolderFile = File(imageFolder)
        val jhoveFolderFile = File(jhoveFolder)

        // Get sorted lists of files
        val altoFiles = altoFolderFile.listFiles { file -> file.extension == "xml" }
            ?.sortedBy { it.name } ?: emptyList()

        val imageFiles = imageFolderFile.listFiles { file -> file.extension == "jp2" }
            ?.sortedBy { it.name } ?: emptyList()

        println("Found ${altoFiles.size} ALTO files and ${imageFiles.size} image files")

        // Build page info for each page
        val pages = altoFiles.zip(imageFiles).mapIndexed { index, (altoFile, imageFile) ->
            val pageNumber = index + 1

            // Extract base name from files
            val imageBaseName = imageFile.nameWithoutExtension
            val altoBaseName = altoFile.nameWithoutExtension

            // Find corresponding JHOVE file
            val jhoveFile = findJhoveFileForImage(jhoveFolderFile, imageFile)
                ?: throw IllegalStateException("Missing JHOVE file for image '${imageFile.name}'")

            val resolution = extractImageDimensionsFromJhove(jhoveFile)

            println("Page $pageNumber: ${imageFile.name} -> ${resolution.width}x${resolution.height}")

            PageInfo(
                pageNumber = pageNumber,
                altoFile = AltoFileInfo(
                    size = altoFile.length(),
                    checksum = calculateMd5(altoFile),
                    urn = "URN:NBN:no-nb_${altoBaseName}",
                    url = "file://ocr/${altoFile.name}"
                ),
                imageFile = ImageFileInfo(
                    size = imageFile.length(),
                    checksum = calculateMd5(imageFile),
                    urn = "URN:NBN:no-nb_${imageBaseName}",
                    url = "file://ocr/${imageFile.name}"
                ),
                resolution = resolution
            )
        }

        // Generate METS v2 + MIX v2.0
        val generator = MetsBrowsingGenerator()
        val metsXml = generator.generateMetsBrowsing(
            objId = tekstUuid,
            pages = pages,
            agentName = "MetsBrowsingGenerator v2",
            version = MetsVersion.METS_2,
            mixVersion = MixVersion.MIX_2_0
        )

        // Write to test resources
        val outputFile = File("$projectFolder/src/test/resources/${tekstUuid}-METS2_BROWSING.xml")
        outputFile.writeText(metsXml)

        println("Generated METS v2 + MIX v2.0 file: ${outputFile.absolutePath}")
        println("File size: ${outputFile.length()} bytes")
    }

    private fun calculateMd5(file: File): String {
        val md = MessageDigest.getInstance("MD5")
        file.inputStream().use { input ->
            val buffer = ByteArray(8192)
            var bytesRead: Int
            while (input.read(buffer).also { bytesRead = it } != -1) {
                md.update(buffer, 0, bytesRead)
            }
        }
        return md.digest().joinToString("") { "%02x".format(it) }
    }

    private fun findJhoveFileForImage(jhoveFolder: File, imageFile: File): File? {
        val imageName = imageFile.name
        val jhoveFileName = "JHOVE_$imageName.xml"
        val jhoveFile = File(jhoveFolder, jhoveFileName)
        return if (jhoveFile.exists()) jhoveFile else null
    }

    private fun extractImageDimensionsFromJhove(jhoveFile: File): ImageResolution {
        try {
            val factory = DocumentBuilderFactory.newInstance()
            factory.isNamespaceAware = true
            val builder = factory.newDocumentBuilder()
            val doc = builder.parse(jhoveFile)

            val xPath = XPathFactory.newInstance().newXPath()

            val widthExpr = "//*[local-name()='imageWidth']/text()"
            val heightExpr = "//*[local-name()='imageHeight']/text()"

            val width = xPath.evaluate(widthExpr, doc)?.toIntOrNull() ?: 2127
            val height = xPath.evaluate(heightExpr, doc)?.toIntOrNull() ?: 3387

            return ImageResolution(width = width, height = height)
        } catch (e: Exception) {
            println("Failed to parse JHOVE file: ${jhoveFile.absolutePath}")
            return ImageResolution(width = 2127, height = 3387)
        }
    }
}
