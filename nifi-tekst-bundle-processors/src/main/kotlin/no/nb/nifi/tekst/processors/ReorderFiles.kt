package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import no.nb.models.RenameInstruction
import no.nb.utils.RenameUtils.renameAll
import no.nb.utils.UUIDv7.randomUUID
import org.apache.nifi.annotation.behavior.DynamicProperty
import org.apache.nifi.annotation.behavior.SideEffectFree
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
@DynamicProperty(
    name = "",
    value = "",
    description = "",
    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
)
@Tags("NB", "Tekst", "Text", "Order", "Files")
@CapabilityDescription(
    "A nifi processor that reorders/renames files in access and primary folders"
)
@SideEffectFree
class ReorderFiles : AbstractProcessor() {
    private var descriptors: MutableList<PropertyDescriptor> = mutableListOf()
    private var relationships: MutableSet<Relationship> = mutableSetOf()

    companion object {
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

        private const val TEKST_PREFIX = "tekst_"
    }

    override fun init(context: ProcessorInitializationContext) {
        descriptors.add(BASE_DIR)
        descriptors.add(RENAME_ZERO_PAD_STRING)
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
        return name.isNotBlank() &&
                !name.contains("..") &&
                !name.contains("/") &&
                !name.contains("\\") &&
                !name.contains("\u0000") // null byte
    }

    /**
     * Ensures the resolved path is strictly within the base directory.
     * Throws SecurityException if path traversal is detected.
     */
    private fun requireWithinBaseDir(baseDirPath: Path, resolvedPath: Path) {
        val normalized = resolvedPath.normalize()
        require(normalized.startsWith(baseDirPath.normalize())) {
            "Path traversal detected: $resolvedPath is outside base directory $baseDirPath"
        }
    }

    /** Example of entries when ItemId=ID1 and zeroPadding="%02d"
     * entries: [
     *   {originalName: ID1_01.jpg, newName: ID1_01.jp2},
     *   {originalName: ID2_04.jpg, newName: ID1_02.jp2}]
     */
    fun addInstruction(
        itemId: String,
        orderedImages: JsonNode?,
        zeroPadding: String,
        baseDirPath: Path
    ): List<RenameInstruction> {
        require(isSafeName(itemId)) { "Invalid itemId: $itemId" }
        val folderName = "$TEKST_PREFIX$itemId"
        val listOfInstructions = mutableListOf<RenameInstruction>()

        orderedImages?.forEachIndexed { index, imageName ->
            val rawName = imageName.asText()
            require(isSafeName(rawName)) { "Invalid image name: $rawName" }
            val prefixedName = if (rawName.startsWith(TEKST_PREFIX)) rawName else "$TEKST_PREFIX$rawName"

            val originalName = if (prefixedName.endsWith(".jp2", ignoreCase = true)) prefixedName else "$prefixedName.jp2"
            val pageNumber = String.format(zeroPadding, index + 1)
            val newName = "$TEKST_PREFIX${itemId}_$pageNumber.jp2"

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
        val ocrDirPath = baseDirPath.resolve(folderName).resolve("representations/access/metadata/other/ocr").normalize()

        // Ensure ocr dir is within base dir
        requireWithinBaseDir(baseDirPath, ocrDirPath)

        val ocrDir = ocrDirPath.toFile()
        if (ocrDir.exists() && ocrDir.isDirectory) {
            ocrDir.listFiles()?.forEach { file ->
                if (file.isFile && file.name.endsWith(".xml", ignoreCase = true)) {
                    // Extra safety: verify each file is within base dir before deleting
                    requireWithinBaseDir(baseDirPath, file.toPath())
                    file.delete()
                }
            }
        }
    }

    override fun onTrigger(context: ProcessContext, session: ProcessSession) {
        var flowFile: FlowFile = session.get() ?: return
        try {
            val zeroPadding = context.getProperty(RENAME_ZERO_PAD_STRING).evaluateAttributeExpressions(flowFile).value ?: "%05d"
            val baseDirValue = context.getProperty(BASE_DIR)
                .evaluateAttributeExpressions(flowFile)
                .value
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

            val mapper = ObjectMapper()
            val rootNode: JsonNode = mapper.readTree(flowFileJson)

            val batchId = rootNode.get("batchId")?.asText()
            val changes = rootNode.get("changes")
            val renameInstructions = mutableListOf<RenameInstruction>()
            val newOrder = mutableListOf<String>()
            val items = mutableListOf<Map<String, Any>>()

            if (changes != null && changes.isArray) {
                for (change in changes) {
                    // Validate and resolve itemId up front in onTrigger
                    var itemId: String = change.get("itemId")?.asText()?.trim() ?: ""
                    if (itemId.isBlank() || itemId == "null") {
                        itemId = randomUUID().toString()
                    }

                    // Validate itemId before any use
                    require(isSafeName(itemId)) { "Invalid itemId: $itemId" }

                    val orderedImages = change.get("orderedImageIds")
                    val itemInstruction = addInstruction(itemId, orderedImages, zeroPadding, baseDirPath)
                    renameInstructions += itemInstruction

                    val itemNewOrder = itemInstruction.map { it.newName }
                    newOrder += itemNewOrder
                    items.add(mapOf("itemId" to itemId, "pages" to itemNewOrder.size))

                    deleteOcrFiles(itemId, baseDirPath)
                }

                logger.info("Reordering files for batchId=$batchId")
                renameAll(baseDirPath, renameInstructions)
                logger.info("Finished reordering files for batchId=$batchId")

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

            session.transfer(flowFile, REL_SUCCESS)

        } catch (exception: Exception) {
            logger.error("Failed to reorder files", exception)
            session.transfer(flowFile, REL_FAILURE)
        }
    }
}