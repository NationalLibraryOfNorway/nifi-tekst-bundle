package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.minio.MinioClient
import no.nb.models.ProcessChangesResult
import no.nb.models.RenameInstruction
import no.nb.utils.PathSafety.isSafeName
import no.nb.utils.PathSafety.requireWithinBaseDir
import no.nb.utils.RenameDiskUtils.renameFilesOnDisk
import no.nb.utils.S3Utils.deleteAllKeysWithPrefix
import no.nb.utils.RenameS3Utils.renameS3Files
import no.nb.nifi.tekst.util.S3ClientFactory.getS3Client
import no.nb.nifi.tekst.util.S3PropertyDescriptors
import no.nb.utils.RenameUtils.extractIdFromFilename
import no.nb.utils.UUIDv7
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.Validator
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.*
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.util.StandardValidators
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.*

@SupportsSensitiveDynamicProperties
@Tags("NB", "Tekst", "Text", "Order", "Files")
@CapabilityDescription(
    "Reorders/renames files in access and primary folders."
)

class ReorderFiles(
    private val uuidProvider: () -> String = { UUIDv7.randomUUID().toString() }
) : AbstractProcessor() {

    private var descriptors: MutableList<PropertyDescriptor> = mutableListOf()
    private var relationships: MutableSet<Relationship> = mutableSetOf()

    companion object {
        private val mapper = ObjectMapper()
        val BASE_DIR: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("base_dir")
            .displayName("Base Directory")
            .description("Base directory for renaming files. Supports Expression Language.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build()

        val RENAME_ZERO_PAD_STRING: PropertyDescriptor = PropertyDescriptor.Builder()
                .name("rename_zero_pad_string")
                .displayName("Zero pad string")
                .description("Format string to use for zero-padded numbers (e.g. '%05d').")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .defaultValue("%05d")
                .build()

        val REL_SUCCESS: Relationship = Relationship.Builder()
                .description("Successfully generated JSON")
                .name("success")
                .build()

        val REL_FAILURE: Relationship = Relationship.Builder()
            .description("Failed processing")
            .name("failure")
            .build()

        val BUCKET: PropertyDescriptor = S3PropertyDescriptors.BUCKET
        val ACCESS_KEY: PropertyDescriptor = S3PropertyDescriptors.ACCESS_KEY
        val SECRET_KEY: PropertyDescriptor = S3PropertyDescriptors.SECRET_KEY
        val REGION: PropertyDescriptor = S3PropertyDescriptors.REGION
        val ENDPOINT: PropertyDescriptor = S3PropertyDescriptors.ENDPOINT

        val PREFIX: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("prefix")
            .displayName("Prefix")
            .description("Optional prefix (folder-like) in S3 placed before tekst_<itemId>/. May be empty.")
            .required(false)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build()

        private const val TEKST_PREFIX = "tekst_"
        private val OCR_WORK_FILE_EXTENSIONS = setOf("rdy", "tkn", "wrk")
    }

    override fun init(context: ProcessorInitializationContext) {
        descriptors.add(BASE_DIR)
        descriptors.add(RENAME_ZERO_PAD_STRING)
        descriptors.add(BUCKET)
        descriptors.add(ACCESS_KEY)
        descriptors.add(SECRET_KEY)
        descriptors.add(REGION)
        descriptors.add(ENDPOINT)
        descriptors.add(PREFIX)
        descriptors = Collections.unmodifiableList(descriptors)

        relationships = HashSet<Relationship>().apply {
            add(REL_SUCCESS)
            add(REL_FAILURE)
        }
        relationships = Collections.unmodifiableSet(relationships)
    }

    override fun getRelationships(): Set<Relationship> = relationships

    public override fun getSupportedPropertyDescriptors(): List<PropertyDescriptor> = descriptors


    /** Example of entries when ItemId=ID1 and zeroPadding="%02d"
     * entries: [
     *   {originalName: ID1_01.tif, newName: ID1_01.tif},
     *   {originalName: ID2_04.tif, newName: ID1_02.tif}]
     */
    fun addInstruction(
        itemId: String, orderedImages: JsonNode?, zeroPadding: String, baseDirPath: Path
    ): List<RenameInstruction> {
        logger.info("Adding rename instructions for itemId='{}', images={}", itemId, orderedImages?.size() ?: 0)
        require(isSafeName(itemId)) { "Invalid itemId: $itemId" }
        val folderName = "$TEKST_PREFIX$itemId"
        val listOfInstructions = mutableListOf<RenameInstruction>()

        orderedImages?.forEachIndexed { index, imageName ->
            val rawName = imageName.asText()
            require(isSafeName(rawName)) { "Invalid image name: $rawName" }
            val prefixedName = if (rawName.startsWith(TEKST_PREFIX)) rawName else "$TEKST_PREFIX$rawName"
            val pageNumber = String.format(zeroPadding, index + 1)

            val hasExplicitExtension = prefixedName.endsWith(".tif", ignoreCase = true)
                    || prefixedName.endsWith(".tiff", ignoreCase = true)
                    || prefixedName.endsWith(".jp2", ignoreCase = true)
                    || prefixedName.substringAfterLast('.', prefixedName) != prefixedName

            if (hasExplicitExtension) {
                val extension = prefixedName.substringAfterLast('.')
                val newName = "$TEKST_PREFIX${itemId}_$pageNumber.$extension"
                requireWithinBaseDir(baseDirPath, baseDirPath.resolve(folderName).resolve(prefixedName))
                requireWithinBaseDir(baseDirPath, baseDirPath.resolve(folderName).resolve(newName))
                listOfInstructions.add(RenameInstruction(prefixedName, newName))
            } else {
                    // No extension given — find ALL matching files on disk and create one instruction per extension.
                    // Probe in the SOURCE folder (derived from the filename itself), not the target folder,
                    // because cross-item moves have source files in a different folder than the target itemId.
                    val sourceFolderName = prefixedName.substringBeforeLast('_')
                    require(isSafeName(sourceFolderName)) { "Invalid source folder derived from filename: $sourceFolderName" }
                    val supportedExtensions = listOf("tif", "tiff", "jp2")
                    val probeDirs = listOf("access", "primary").map {
                        baseDirPath.resolve(sourceFolderName).resolve("representations/$it/data")
                    }
                    val discoveredExtensions = supportedExtensions.filter { ext ->
                        probeDirs.any { dir -> Files.exists(dir.resolve("$prefixedName.$ext")) }
                    }

                    if (discoveredExtensions.isEmpty()) {
                        logger.warn(
                            "No file found on disk for '{}' with any supported extension, defaulting to .tif", prefixedName
                        )
                        val originalName = "$prefixedName.tif"
                        val newName = "$TEKST_PREFIX${itemId}_$pageNumber.tif"
                        requireWithinBaseDir(baseDirPath, baseDirPath.resolve(sourceFolderName).resolve(originalName))
                        requireWithinBaseDir(baseDirPath, baseDirPath.resolve(folderName).resolve(newName))
                        listOfInstructions.add(RenameInstruction(originalName, newName))
                    } else {
                        logger.debug(
                            "Discovered extensions {} for '{}', adding one rename instruction per extension",
                            discoveredExtensions, prefixedName
                        )
                        discoveredExtensions.forEach { ext ->
                            val originalName = "$prefixedName.$ext"
                            val newName = "$TEKST_PREFIX${itemId}_$pageNumber.$ext"
                            requireWithinBaseDir(baseDirPath, baseDirPath.resolve(sourceFolderName).resolve(originalName))
                            requireWithinBaseDir(baseDirPath, baseDirPath.resolve(folderName).resolve(newName))
                            listOfInstructions.add(RenameInstruction(originalName, newName))
                        }
                }
            }
        }

        return listOfInstructions
    }

    /**
     * Detects source folders that have been fully emptied by the rename — i.e. itemIds that
     * appear only as a source (not also as a target) and whose `representations/access/data`
     * and `representations/primary/data` directories are now empty on disk.
     *
     * For each such folder, removes the entire `tekst_<id>` directory tree on disk
     * (data dirs, OCR XMLs, anything else under it) and deletes all S3 keys under
     * `<prefix>/tekst_<id>/`.
     */
    private fun cleanupEmptiedSourceFolders(
        baseDirPath: Path,
        renameInstructions: List<RenameInstruction>,
        client: MinioClient,
        bucket: String,
        prefix: String
    ) {
        val sourceFolders = renameInstructions.mapNotNull { extractIdFromFilename(it.originalName) }.toSet()
        val targetFolders = renameInstructions.mapNotNull { extractIdFromFilename(it.newName) }.toSet()
        val candidates = sourceFolders - targetFolders

        for (folder in candidates) {
            require(isSafeName(folder)) { "Invalid source folder name: $folder" }
            val itemDir = baseDirPath.resolve(folder).normalize()
            requireWithinBaseDir(baseDirPath, itemDir)

            val accessData = itemDir.resolve("representations/access/data")
            val primaryData = itemDir.resolve("representations/primary/data")

            val accessEmpty = !Files.exists(accessData) ||
                    Files.list(accessData).use { stream -> !stream.findAny().isPresent }
            val primaryEmpty = !Files.exists(primaryData) ||
                    Files.list(primaryData).use { stream -> !stream.findAny().isPresent }

            if (!(accessEmpty && primaryEmpty)) {
                logger.debug(
                    "Source folder '{}' is not fully emptied (access empty={}, primary empty={}); skipping cleanup",
                    folder, accessEmpty, primaryEmpty
                )
                continue
            }

            logger.info("Source folder '{}' fully emptied by rename — cleaning up disk and S3", folder)
            if (Files.exists(itemDir)) {
                val deleted = itemDir.toFile().deleteRecursively()
                if (!deleted) logger.warn("Failed to fully delete emptied source folder: {}", itemDir)
            }
            val keyPrefix = if (prefix.isBlank()) "$folder/" else "$prefix/$folder/"
            deleteAllKeysWithPrefix(client, bucket, keyPrefix)
        }
    }

    fun deleteOcrFiles(itemId: String, baseDirPath: Path) {
        // itemId must be validated before calling this function
        require(isSafeName(itemId)) { "Invalid itemId: $itemId" }
        val folderName = "$TEKST_PREFIX$itemId"

        // Delete OCR files from both access and primary representations
        listOf("access", "primary").forEach { representation ->
            val ocrDirPath =
                baseDirPath.resolve(folderName).resolve("representations/$representation/metadata/other/ocr").normalize()

            logger.info("Attempting to delete OCR files for itemId={} in representation='{}': {}", itemId, representation, ocrDirPath)

            // Ensure ocr dir is within base dir
            requireWithinBaseDir(baseDirPath, ocrDirPath)

            val ocrDir = ocrDirPath.toFile()
            if (ocrDir.exists() && ocrDir.isDirectory) {
                val entries = ocrDir.listFiles()?.toList() ?: emptyList()
                logger.info("Found {} entries to delete in {}", entries.size, ocrDirPath)
                entries.forEach { entry ->
                    requireWithinBaseDir(baseDirPath, entry.toPath())
                    entry.deleteRecursively()
                    logger.debug("Deleted OCR entry '{}'", entry.absolutePath)
                }
            } else {
                logger.debug("OCR directory does not exist or is not a directory: {}", ocrDirPath)
            }
        }
    }

    /**
     * Deletes OCR work files (e.g. `.rdy`, `.tkn`, `.wrk`) recursively across the item directory.
     * These are transient files produced during OCR processing and are not part of the final output.
     */
    fun deleteOcrWorkFiles(itemId: String, baseDirPath: Path) {
        require(isSafeName(itemId)) { "Invalid itemId: $itemId" }
        val itemDir = baseDirPath.resolve("$TEKST_PREFIX$itemId").normalize()
        requireWithinBaseDir(baseDirPath, itemDir)

        if (!Files.exists(itemDir) || !Files.isDirectory(itemDir)) {
            logger.debug("Item directory does not exist or is not a directory for OCR work file cleanup: {}", itemDir)
            return
        }

        val workFiles = Files.walk(itemDir).use { stream ->
            stream.filter { path ->
                Files.isRegularFile(path) && OCR_WORK_FILE_EXTENSIONS.contains(path.fileName.toString().substringAfterLast('.', "").lowercase())
            }.toList()
        }

        logger.info("Found {} OCR work files to delete under {}", workFiles.size, itemDir)
        workFiles.forEach { file ->
            requireWithinBaseDir(baseDirPath, file)
            Files.deleteIfExists(file)
            logger.debug("Deleted OCR work file '{}'", file)
        }
    }

    /**
     * Processes the changes array from the flowfile JSON, building rename instructions
     * for each item. Generates a new UUID for items with missing or null itemIds.
     */
    private fun processChanges(
        changes: JsonNode, zeroPadding: String, baseDirPath: Path
    ): ProcessChangesResult {
        val renameInstructions = mutableListOf<RenameInstruction>()
        val itemIds = mutableListOf<String>()
        val items = mutableListOf<Map<String, Any>>()

        for (change in changes) {
            val itemId = change.get("itemId")
                ?.takeUnless { it.isNull }
                ?.asText()?.trim()
                ?.takeUnless { it.isBlank() || it.equals("null", ignoreCase = true) }
                ?: uuidProvider()

            require(isSafeName(itemId)) { "Invalid itemId: $itemId" }
            itemIds.add(itemId)

            val orderedImages = change.get("orderedImageIds")
            val itemInstruction = addInstruction(itemId, orderedImages, zeroPadding, baseDirPath)
            renameInstructions += itemInstruction
            items.add(mapOf("itemId" to itemId, "pages" to itemInstruction.size))
        }
        return ProcessChangesResult(renameInstructions, itemIds, items)
    }

    override fun onTrigger(context: ProcessContext, session: ProcessSession) {
        var flowFile: FlowFile = session.get() ?: return

        try {
            val bucket = context.getProperty(BUCKET).evaluateAttributeExpressions(flowFile).value
            val accessKey = context.getProperty(ACCESS_KEY).evaluateAttributeExpressions(flowFile).value
            val secretKey = context.getProperty(SECRET_KEY).value
            val region = context.getProperty(REGION).evaluateAttributeExpressions(flowFile).value
            val endpoint = context.getProperty(ENDPOINT).evaluateAttributeExpressions(flowFile).value
            val prefix = context.getProperty(PREFIX).evaluateAttributeExpressions(flowFile).value?.trimEnd('/') ?: ""
            val client = getS3Client(accessKey, secretKey, region, endpoint)
            val zeroPadding =
                context.getProperty(RENAME_ZERO_PAD_STRING).evaluateAttributeExpressions(flowFile).value ?: "%05d"
            val baseDirValue = context.getProperty(BASE_DIR).evaluateAttributeExpressions(flowFile).value
                ?: throw ProcessException("base_dir property is missing")

            val baseDirFile = File(baseDirValue).canonicalFile
            val baseDirPath = baseDirFile.toPath().normalize()

            if (!baseDirFile.exists() || !baseDirFile.isDirectory) {
                throw ProcessException("Invalid base directory: $baseDirFile")
            }

            flowFile = session.putAttribute(flowFile, "mime.type", "application/json")

            var flowFileJson = ""
            session.read(flowFile) { input ->
                flowFileJson = input.bufferedReader(StandardCharsets.UTF_8).readText()
            }

            val rootNode: JsonNode = mapper.readTree(flowFileJson)
            val batchId = rootNode.get("batchId")?.takeIf { !it.isNull }?.asText()
            val changes = rootNode.get("changes")

            logger.info("Processing batchId='{}', changes={}", batchId, changes?.size() ?: 0)

            if (changes != null && changes.isArray) {
                val (renameInstructions, itemIds, items) = processChanges(changes, zeroPadding, baseDirPath)

                logger.debug("Reordering {} files on disk for batchId='{}'", renameInstructions.size, batchId)
                renameFilesOnDisk(baseDirPath, renameInstructions)
                logger.info("Reordered {} files on disk for batchId='{}'", renameInstructions.size, batchId)

                try {
                    logger.debug("Reordering {} files on S3 for batchId='{}'", renameInstructions.size, batchId)
                    renameS3Files(client, bucket, renameInstructions, prefix)
                    logger.info("Reordered {} files on S3 for batchId='{}'", renameInstructions.size, batchId)
                } catch (e: Exception) {
                    logger.error("S3 rename failed, attempting disk rollback for batchId='{}'", batchId, e)
                    val rollbackInstructions = renameInstructions.map {
                        RenameInstruction(originalName = it.newName, newName = it.originalName)
                    }
                    renameFilesOnDisk(baseDirPath, rollbackInstructions)
                    throw e
                }

                val sourceItemIds = renameInstructions
                    .mapNotNull { extractIdFromFilename(it.originalName) }
                    .map { it.removePrefix(TEKST_PREFIX) }
                val allItemIdsForOcrCleanup = (itemIds + sourceItemIds).toSet()
                allItemIdsForOcrCleanup.forEach { itemId ->
                    deleteOcrFiles(itemId, baseDirPath)
                    deleteOcrWorkFiles(itemId, baseDirPath)
                }

                cleanupEmptiedSourceFolders(baseDirPath, renameInstructions, client, bucket, prefix)

                val outputJson = mapper.writeValueAsString(
                    mapOf(
                        "batchId" to batchId,
                        "items" to items,
                        "font" to rootNode.get("font")?.asText(),
                        "materialType" to rootNode.get("materialType")?.asText(),
                        "publicationType" to rootNode.get("publicationType")?.asText(),
                        "language" to rootNode.get("language")?.asText(),
                        "digital" to rootNode.get("digital")?.asBoolean()
                    )
                )

                flowFile = session.write(flowFile) { out ->
                    out.write(outputJson.toByteArray(StandardCharsets.UTF_8))
                }
            }

            logger.info("Successfully processed batchId='{}'", batchId)
            session.transfer(flowFile, REL_SUCCESS)

        } catch (exception: Exception) {
            logger.error("Failed to reorder files", exception)
            session.transfer(flowFile, REL_FAILURE)
        }
    }
}