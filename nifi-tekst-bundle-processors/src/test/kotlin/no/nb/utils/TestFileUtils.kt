package no.nb.utils

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.File
import java.nio.file.Path

object TestFileUtils {
    private val mapper = ObjectMapper()

    fun createFile(baseDir: Path, repType: String, filename: String, content: String = "data"): File {
        val folderName = filename.substringBeforeLast('_')
        val dir = File(baseDir.toFile(), "$folderName/representations/$repType/data").apply { mkdirs() }
        return File(dir, filename).apply { writeText(content) }
    }

    fun readJson(fileName: String): JsonNode {
        val resource = this::class.java.classLoader.getResource("reorder-files/$fileName")
        requireNotNull(resource) { "Resource not found" }
        val jsonContent = File(resource.toURI()).readText()
        return mapper.readTree(jsonContent)
    }
}