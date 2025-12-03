package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.nifi.annotation.behavior.DynamicProperty
import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.AllowableValue
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.ProcessorInitializationContext
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.util.StandardValidators
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.HashSet

@SupportsSensitiveDynamicProperties
@DynamicProperty(
    name = "JSON path",
    value = "Value or expression",
    description = "Properties named like 'foo.bar' will be turned into nested JSON {\"foo\": {\"bar\": ...}}",
    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
)
@Tags("NB", "Tekst", "Text", "Generate", "JSON")
@CapabilityDescription(
    "Generates nested JSON from dynamic processor properties with dotted names. " +
            "Writes JSON to the FlowFile content. output_mode decides whether to discard or merge with existing JSON in the FlowFile. " +
            "Supports arrays using bracket-index syntax (e.g. items[0].name) to create JSON lists; when merging, object fields are merged recursively, but arrays and non-object nodes are overwritten by the generated values."
)
@SideEffectFree
class GenerateJsonFromProps : AbstractProcessor() {
    private var descriptors: MutableList<PropertyDescriptor> = mutableListOf()

    private var relationships: MutableSet<Relationship> = mutableSetOf()

    companion object {

        private val MODE_DISCARD = AllowableValue(
            "discard",
            "Discard existing content",
            "Replace existing FlowFile content with generated JSON."
        )

        private val MODE_MERGE = AllowableValue(
            "merge",
            "Merge with existing JSON",
            "Read existing FlowFile content as JSON and merge with generated JSON."
        )

        val OUTPUT_MODE: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("output_mode")
            .displayName("Output mode")
            .description("Choose whether to discard existing content or merge with it.")
            .allowableValues(MODE_DISCARD, MODE_MERGE)
            .defaultValue(MODE_DISCARD.value)
            .required(true)
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
        descriptors.add(OUTPUT_MODE)
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

    override fun getSupportedDynamicPropertyDescriptor(propertyName: String): PropertyDescriptor {
        return PropertyDescriptor.Builder()
            .name(propertyName)
            .displayName(propertyName)
            .description("Dynamic JSON path property '$propertyName'")
            .dynamic(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build()
    }

    override fun onTrigger(context: ProcessContext, session: ProcessSession) {
        var flowFile: FlowFile = session.get() ?: return

        val outputMode = context.getProperty(OUTPUT_MODE).value ?: MODE_DISCARD.value

        val mapper = ObjectMapper()
        val factory = JsonNodeFactory.instance
        val newRoot: ObjectNode = factory.objectNode()

        try {
            // Build JSON from dynamic properties only (skip known static descriptors)
            val staticDescriptors = descriptors.toSet()
            val allProps: Map<PropertyDescriptor, String> = context.properties

            for ((descriptor, _) in allProps) {
                if (staticDescriptors.contains(descriptor)) {
                    continue
                }

                val path = descriptor.name        // e.g. "metadata.title.value"
                val value = context.getProperty(descriptor)
                    .evaluateAttributeExpressions(flowFile)
                    .value

                if (value != null) {
                    putNested(newRoot, path, value, factory)
                }
            }

            // Decide how to write to FlowFile
            flowFile = if (outputMode == MODE_MERGE.value) {
                mergeWithExisting(flowFile, session, mapper, factory, newRoot)
            } else {
                overwriteContent(flowFile, session, newRoot)
            }
            flowFile = session.putAttribute(flowFile, "mime.type", "application/json")
            session.transfer(flowFile, REL_SUCCESS)
        } catch (e: Exception) {
            logger.error("Failed to generate JSON", e)
            session.transfer(flowFile, REL_FAILURE)
        }
    }

    private fun overwriteContent(
        flowFile: FlowFile,
        session: ProcessSession,
        newRoot: ObjectNode
    ): FlowFile {
        val jsonBytes = newRoot.toString().toByteArray(StandardCharsets.UTF_8)
        return session.write(flowFile) { out ->
            out.write(jsonBytes)
        }
    }

    private fun mergeWithExisting(
        flowFile: FlowFile,
        session: ProcessSession,
        mapper: ObjectMapper,
        factory: JsonNodeFactory,
        newRoot: ObjectNode
    ): FlowFile {
        return session.write(flowFile) { input, output ->
            val existingStr = input.bufferedReader(StandardCharsets.UTF_8).readText()

            val existingNode: JsonNode = if (existingStr.isNotBlank()) {
                try {
                    mapper.readTree(existingStr)
                } catch (e: IOException) {
                    // Not valid JSON, treat as empty object
                    logger.warn("Existing content is not valid JSON, overwriting", e)
                    factory.objectNode()
                }
            } else {
                factory.objectNode()
            }

            val merged = mergeJson(existingNode, newRoot, factory)
            output.write(merged.toString().toByteArray(StandardCharsets.UTF_8))
        }
    }

    /**
     * Convert a dotted path like "metadata.title.value" to nested JSON:
     * { "metadata": { "title": { "value": <value> } } }
     *
     * This version treats all segments as object keys. If you want array support
     * (e.g. "identifier[0]"), extend this function to detect indices.
     */
    private fun putNested(root: ObjectNode, path: String, value: String, factory: JsonNodeFactory) {
        val parts = path.split('.')
        var current: JsonNode = root

        val arrayRegex = Regex("^(.+)\\[(\\d+)]$")

        for (i in 0 until parts.size - 1) {
            val segment = parts[i]
            val arrayMatch = arrayRegex.find(segment)

            if (arrayMatch != null) {
                val key = arrayMatch.groupValues[1]
                val index = arrayMatch.groupValues[2].toInt()

                // current must be an ObjectNode to hold a named array
                if (current is ObjectNode) {
                    val existing = current.get(key)
                    val arr = if (existing is ArrayNode) existing else {
                        val newArr = factory.arrayNode()
                        current.set<JsonNode>(key, newArr)
                        newArr
                    }

                    // ensure array has element at index
                    while (arr.size() <= index) arr.addNull()

                    val elem = arr.get(index)
                    if (elem is ObjectNode) {
                        current = elem
                    } else {
                        val newObj = factory.objectNode()
                        arr.set(index, newObj)
                        current = newObj
                    }
                } else {
                    // unexpected structure: overwrite with object containing array
                    val replacement = factory.objectNode()
                    current = replacement
                }
            } else {
                // plain object key
                if (current is ObjectNode) {
                    val child = current.get(segment)
                    current = if (child is ObjectNode) {
                        child
                    } else {
                        val newChild = factory.objectNode()
                        current.set<JsonNode>(segment, newChild)
                        newChild
                    }
                } else {
                    // unexpected: create a new object and continue
                    val newObj = factory.objectNode()
                    current = newObj
                }
            }
        }

        // handle last segment (set the value)
        val last = parts.last()
        val lastArrayMatch = arrayRegex.find(last)
        if (lastArrayMatch != null) {
            val key = lastArrayMatch.groupValues[1]
            val index = lastArrayMatch.groupValues[2].toInt()

            if (current is ObjectNode) {
                val existing = current.get(key)
                val arr = if (existing is ArrayNode) existing else {
                    val newArr = factory.arrayNode()
                    current.set<JsonNode>(key, newArr)
                    newArr
                }
                while (arr.size() <= index) arr.addNull()
                arr.set(index, factory.textNode(value))
            }
        } else {
            if (current is ObjectNode) {
                current.put(last, value)
            }
        }
    }

    /**
     * Simple recursive object/object merge:
     * - If both nodes are objects, merge field by field.
     * - Otherwise, the "added" node overwrites the existing one.
     */
    private fun mergeJson(existing: JsonNode, added: JsonNode, factory: JsonNodeFactory): JsonNode {
        if (!existing.isObject || !added.isObject) {
            return added
        }

        val result = (existing.deepCopy() as ObjectNode)

        val fields = added.fields()
        while (fields.hasNext()) {
            val (name, value) = fields.next()
            val old = result.get(name)
            val mergedChild = if (old != null) {
                mergeJson(old, value, factory)
            } else {
                value
            }
            result.set<JsonNode>(name, mergedChild)
        }

        return result
    }
}
