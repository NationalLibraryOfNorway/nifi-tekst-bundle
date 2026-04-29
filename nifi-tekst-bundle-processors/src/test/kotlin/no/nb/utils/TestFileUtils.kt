package no.nb.utils

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import no.nb.utils.RenameUtils.extractIdFromFilename
import java.io.File
import java.nio.file.Path

object TestFileUtils {

    private val mapper = ObjectMapper()

    fun readJson(fileName: String): JsonNode {
        val resource = this::class.java.classLoader.getResource("reorder-files/$fileName")
        requireNotNull(resource) { "Resource not found" }
        return mapper.readTree(File(resource.toURI()).readText())
    }

    fun createFile(baseDir: Path, repType: String, filename: String, content: String = "data"): File {
        val folderName = filename.substringBeforeLast('_')
        val dir = File(baseDir.toFile(), "$folderName/representations/$repType/data").apply { mkdirs() }
        return File(dir, filename).apply { writeText(content) }
    }

    /**
     * Creates tif files on disk for both access and primary representations.
     * Returns the created File references so tests can verify their existence.
     */
    fun createDiskFiles(changes: JsonNode, baseDir: Path): List<File> {
        val createdFiles = mutableListOf<File>()
        changes.forEach { change ->
            val orderedImages = change["orderedImageIds"] ?: return@forEach
            orderedImages.forEach { imageName ->
                val rawName = imageName.asText()
                val fileName = if (rawName.endsWith(".tif")) rawName else "$rawName.tif"
                createdFiles.add(createFile(baseDir, "access", fileName))
                createdFiles.add(createFile(baseDir, "primary", fileName))
            }
        }
        return createdFiles
    }

    /**
     * Creates OCR xml files for each change under the tekst_ folder structure.
     * Returns the created File references so tests can verify deletion.
     */
    fun createOcrFiles(changes: JsonNode, baseDir: Path, itemIdOverride: String? = null): List<File> {
        val createdFiles = mutableListOf<File>()
        changes.forEach { change ->
            val orderedImages = change["orderedImageIds"] ?: return@forEach
            val itemId = change["itemId"]?.asText() ?: itemIdOverride
            val ocrDir = baseDir.resolve("tekst_$itemId/representations/access/metadata/other/ocr").toFile()
            ocrDir.mkdirs()
            orderedImages.forEach { imageName ->
                val rawName = imageName.asText()
                val xmlFile = File(ocrDir, "$rawName.xml").apply { writeText("<xml>dummy</xml>") }
                createdFiles.add(xmlFile)
            }
        }
        return createdFiles
    }

    /**
     * Creates S3 objects mirroring the disk structure.
     * Returns the created S3 keys so tests can verify rename/deletion.
     */
    fun createS3Files(
        changes: JsonNode,
        minioTestBase:
        MinIOTestBase,
        prefix: String
    ): List<String> {
        val createdKeys = mutableListOf<String>()
        changes.forEach { change ->
            val orderedImages = change["orderedImageIds"] ?: return@forEach
            orderedImages.forEach { imageName ->
                val rawName = imageName.asText()
                val fileName = if (rawName.endsWith(".tif")) rawName else "$rawName.tif"
                val fileItemId = extractIdFromFilename(fileName)
                    ?: throw IllegalStateException("Could not extract id from $fileName")

                val accessKey = "$prefix/$fileItemId/representations/access/data/$fileName"
                val primaryKey = "$prefix/$fileItemId/representations/primary/data/$fileName"
                minioTestBase.putObject(accessKey)
                minioTestBase.putObject(primaryKey)
                createdKeys.add(accessKey)
                createdKeys.add(primaryKey)
            }
        }
        return createdKeys
    }
}