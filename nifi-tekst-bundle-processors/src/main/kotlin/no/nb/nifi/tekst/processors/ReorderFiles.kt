package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import no.nb.models.Entry
import no.nb.utils.renameAll
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

    override fun getRelationships(): Set<Relationship> {
        return relationships
    }

    public override fun getSupportedPropertyDescriptors(): List<PropertyDescriptor> {
        return descriptors
    }

   /** Example of entries when ItemId=ID1 and zeroPadding="%02d"
    * entries: [
    *   {originalName: ID1_01.jpg, newName: ID1_01.jpt},
    *   {originalName: ID2_04.jpg, newName: ID1:02.jpg}]
    */
   fun addEntries(itemId: String, newOrder: JsonNode?, zeroPadding: String): List<Entry> {
       val entries = mutableListOf<Entry>()
       newOrder?.forEachIndexed { index, imageName ->
           var originalName = imageName.asText()
           if (!originalName.endsWith(".jp2", ignoreCase = true)) {
               originalName += ".jp2"
           }
           val pageNumber = String.format(zeroPadding, index + 1)
           val newName = "${itemId}_$pageNumber.jp2"
           entries.add(Entry(originalName, newName))
       }
       return entries
   }

    fun deleteOcr(itemId: String, baseDir: File) {
        val ocrDir = File(baseDir,"/$itemId/access/metadata/other/ocr")
        if (ocrDir.exists() && ocrDir.isDirectory) {
            ocrDir.listFiles()?.forEach { file ->
                if (file.isFile) file.delete()
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

            if (!baseDirFile.exists() || !baseDirFile.isDirectory) {
                throw ProcessException("Invalid base directory: $baseDirFile")
            }

            // Set mime.type to application/json
            flowFile = session.putAttribute(flowFile, "mime.type", "application/json")

            // Read the content of the flowfile as JSON
            var jsonStr = ""
            session.read(flowFile) { input ->
                jsonStr = input.bufferedReader(StandardCharsets.UTF_8).readText()
            }

            val mapper = ObjectMapper()
            val rootNode: JsonNode = mapper.readTree(jsonStr)

            val batchId = rootNode.get("batchId")?.asText()
            val changes = rootNode.get("change")
            if (changes != null && changes.isArray) {
                for (change in changes) {
                    val itemId: String = change.get("itemId").asText() ?: ""
                    val newOrder = change.get("newOrder")
                    val entries: List<Entry> = addEntries(itemId, newOrder, zeroPadding)
                    logger.info("Reordering files for ItemId=$itemId")
                    renameAll(entries, baseDirFile)
                    logger.info("Finished reordering files for ItemId=$itemId")
                    deleteOcr(itemId, baseDirFile)
                }
            }
            session.transfer(flowFile, REL_SUCCESS)

        } catch (exception: Exception) {
            logger.error("Failed to reorder files", exception)
            session.transfer(flowFile, REL_FAILURE)
        }
    }

}