package no.nb.nifi.tekst.processors

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

object TestObjectFolderHelper {
    fun createTempObjectFolder(
        fixtureRoot: Path,
        descriptiveXml: Path
    ): Path {
        val objectFolder = Files.createTempDirectory("jhove_object_")

        // Primary data: representations/primary/data
        copyDir(fixtureRoot.resolve("representations/primary/data"), objectFolder.resolve("representations/primary/data"))

        // Access data: representations/access/data
        copyDir(fixtureRoot.resolve("representations/access/data"), objectFolder.resolve("representations/access/data"))

        // OCR data: representations/access/metadata/other/ocr
        copyDir(fixtureRoot.resolve("representations/access/metadata/other/ocr"), objectFolder.resolve("representations/access/metadata/other/ocr"))

        val descriptiveTarget = objectFolder.resolve("metadata/descriptive")
        Files.createDirectories(descriptiveTarget)
        Files.copy(
            descriptiveXml,
            descriptiveTarget.resolve(descriptiveXml.fileName.toString()),
            StandardCopyOption.REPLACE_EXISTING
        )

        return objectFolder
    }

    fun deleteTempObjectFolder(objectFolder: Path) {
        objectFolder.toFile().deleteRecursively()
    }


    private fun copyDir(source: Path, target: Path) {
        if (!Files.exists(source)) {
            return
        }
        Files.walk(source).use { stream ->
            stream.forEach { path ->
                val relative = source.relativize(path)
                val destination = target.resolve(relative)
                if (Files.isDirectory(path)) {
                    Files.createDirectories(destination)
                } else {
                    Files.createDirectories(destination.parent)
                    Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING)
                }
            }
        }
    }
}

