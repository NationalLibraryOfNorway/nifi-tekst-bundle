package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.minio.MinioClient
import no.nb.models.ProcessChangesResult
import no.nb.models.RenameInstruction
import no.nb.utils.RenameDiskUtils.renameFilesOnDisk
import no.nb.utils.RenameS3Utils.renameS3Files
import no.nb.nifi.tekst.util.S3ClientFactory.getS3Client
import no.nb.utils.UUIDv7
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.*
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.util.StandardValidators
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.*

@SupportsSensitiveDynamicProperties
@Tags("NB", "Tekst", "Text", "Order", "Files")
@CapabilityDescription(
    "A nifi processor that reorders/renames files in access and primary folders"
)

class ReorderFiles(
    private val uuidProvider: () -> String = { UUIDv7.randomUUID().toString() },
    private val s3ClientProvider: ((accessKey: String, secretKey: String, region: String, endpoint: String) -> MinioClient)? = null
) : AbstractProcessor() {

    private var descriptors: MutableList<PropertyDescriptor> = mutableListOf()
    private var relationships: MutableSet<Relationship> = mutableSetOf()

    companion object {
        private val mapper = ObjectMapper()
        val BASE_DIR: PropertyDescriptor = PropertyDescriptor.Builder().name("base_dir").displayName("Base Directory")
            .description("Base directory for renaming files. Supports Expression Language.").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build()

        val RENAME_ZERO_PAD_STRING: PropertyDescriptor =
            PropertyDescriptor.Builder().name("rename_zero_pad_string").displayName("Zero pad string")
                .description("Format string to use for zero-padded numbers (e.g. '%05d').").required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).defaultValue("%05d").build()

        val REL_SUCCESS: Relationship =
            Relationship.Builder().description("Successfully generated JSON").name("success").build()

        val REL_FAILURE: Relationship = Relationship.Builder().description("Failed processing").name("failure").build()

        val BUCKET: PropertyDescriptor =
            PropertyDescriptor.Builder().name("bucket").displayName("S3 bucket").description("S3 bucket name")
                .required(true).addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build()

        val ACCESS_KEY: PropertyDescriptor =
            PropertyDescriptor.Builder().name("access_key").displayName("S3 access key").description("S3 access key")
                .required(true).addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build()

        val SECRET_KEY: PropertyDescriptor =
            PropertyDescriptor.Builder().name("secret_key").displayName("S3 secret key").description("S3 secret key")
                .required(true).sensitive(true).addValidator(StandardValidators.NON_BLANK_VALIDATOR).build()

        val REGION: PropertyDescriptor =
            PropertyDescriptor.Builder().name("region").displayName("S3 region").description("S3 region").required(true)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build()

        val ENDPOINT: PropertyDescriptor =
            PropertyDescriptor.Builder().name("endpoint").displayName("Endpoint").description("S3 endpoint (url)")
                .required(true).addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build()

        val PREFIX: PropertyDescriptor = PropertyDescriptor.Builder().name("prefix").displayName("Prefix")
            .description("Prefix (folder-like) in S3 that contains the files to download")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build()

        private const val TEKST_PREFIX = "tekst_"
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

    /**
     * Validates that a name is safe to use as a path component.
     * Disallows path traversal sequences and path separator characters.
     */
    private fun isSafeName(name: String): Boolean {
        val safe =
            name.isNotBlank() && !name.contains("..") && !name.contains("/") && !name.contains("\\") && !name.contains("\u0000")
        if (!safe) {
            logger.warn("Validation failed for name: '{}'", name)
        }
        return safe
    }

    /**
     * Ensures the resolved path is strictly within the base directory.
     * Throws SecurityException if path traversal is detected.
     */
    private fun requireWithinBaseDir(baseDirPath: Path, resolvedPath: Path) {
        val normalized = resolvedPath.normalize()
        if (!normalized.startsWith(baseDirPath.normalize())) {
            logger.error("Path traversal detected: {} is outside base directory {}", resolvedPath, baseDirPath)
            throw SecurityException("Path traversal detected: $resolvedPath is outside base directory $baseDirPath")
        }
    }

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

            val originalName = when {
                prefixedName.endsWith(".tif", ignoreCase = true) -> prefixedName
                prefixedName.endsWith(".tiff", ignoreCase = true) -> prefixedName
                else -> "$prefixedName.tif"  // default to .tif if no extension
            }
            val pageNumber = String.format(zeroPadding, index + 1)
            val extension = originalName.substringAfterLast('.')
            val newName = "$TEKST_PREFIX${itemId}_$pageNumber.$extension"

            // Verify both original and new paths stay within base dir
            requireWithinBaseDir(baseDirPath, baseDirPath.resolve(folderName).resolve(originalName))
            requireWithinBaseDir(baseDirPath, baseDirPath.resolve(folderName).resolve(newName))

            listOfInstructions.add(RenameInstruction(originalName, newName))
        }

        return listOfInstructions
    }

    fun deleteOcrFiles(itemId: String, baseDirPath: Path) {
        // itemId must be validated before calling this function
        require(isSafeName(itemId)) { "Invalid itemId: $itemId" }
        val folderName = "$TEKST_PREFIX$itemId"
        val ocrDirPath =
            baseDirPath.resolve(folderName).resolve("representations/access/metadata/other/ocr").normalize()

        logger.info("Attempting to delete OCR files for itemId={} in directory: {}", itemId, ocrDirPath)

        // Ensure ocr dir is within base dir
        requireWithinBaseDir(baseDirPath, ocrDirPath)

        val ocrDir = ocrDirPath.toFile()
        if (ocrDir.exists() && ocrDir.isDirectory) {
            val files =
                ocrDir.listFiles()?.filter { it.isFile && it.name.endsWith(".xml", ignoreCase = true) } ?: emptyList()
            logger.info("Found {} OCR XML files to delete in {}", files.size, ocrDirPath)
            files.forEach { file ->
                requireWithinBaseDir(baseDirPath, file.toPath())
                file.delete()
                logger.debug("Deleted OCR file '{}'", file.absolutePath)
            }
        } else {
            logger.info("OCR directory does not exist or is not a directory: {}", ocrDirPath)
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
            val itemIdNode = change.get("itemId")
            var itemId = if (itemIdNode == null || itemIdNode.isNull) {
                ""
            } else {
                itemIdNode.asText().trim()
            }
            if (itemId.isBlank() || itemId.equals("null", ignoreCase = true)) itemId = uuidProvider()
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

        val bucket = context.getProperty(BUCKET).evaluateAttributeExpressions(flowFile).value
        val accessKey = context.getProperty(ACCESS_KEY).evaluateAttributeExpressions(flowFile).value
        val secretKey = context.getProperty(SECRET_KEY).value
        val region = context.getProperty(REGION).evaluateAttributeExpressions(flowFile).value
        val endpoint = context.getProperty(ENDPOINT).evaluateAttributeExpressions(flowFile).value
        val prefix = context.getProperty(PREFIX).evaluateAttributeExpressions(flowFile).value.trimEnd('/')
        val client = s3ClientProvider?.invoke(accessKey, secretKey, region, endpoint) ?: getS3Client(
            accessKey,
            secretKey,
            region,
            endpoint
        )
        try {
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

                itemIds.forEach { itemId -> deleteOcrFiles(itemId, baseDirPath) }

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