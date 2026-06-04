package no.nb.nifi.tekst.processors

import no.nb.nifi.tekst.util.S3ClientFactory.getS3Client
import no.nb.nifi.tekst.util.S3PropertyDescriptors
import no.nb.utils.PathSafety.isSafeName
import no.nb.utils.S3Utils
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
import org.slf4j.LoggerFactory
import java.util.*

@SupportsSensitiveDynamicProperties
@Tags("NB", "Tekst", "Text", "Delete", "Files")
@CapabilityDescription(
    "A nifi processor that recursively deletes all files under tekst_<itemId>/ in a MinIO/S3 bucket."
)
class DeleteMinioFile : AbstractProcessor() {

    private var descriptors: MutableList<PropertyDescriptor> = mutableListOf()
    private var relationships: MutableSet<Relationship> = mutableSetOf()
    private val log = LoggerFactory.getLogger(DeleteMinioFile::class.java)

    companion object {
        val ITEM_ID: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("item_id")
            .displayName("Item ID")
            .description("Item id whose folder (tekst_<itemId>/) will be recursively deleted in the bucket.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build()

        val REL_SUCCESS: Relationship = Relationship.Builder()
            .description("Successfully deleted all keys for the item")
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
    }

    override fun init(context: ProcessorInitializationContext) {
        descriptors.add(ITEM_ID)
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


    override fun onTrigger(context: ProcessContext, session: ProcessSession) {
        val flowFile: FlowFile = session.get() ?: return

        try {
            val rawItemId = context.getProperty(ITEM_ID).evaluateAttributeExpressions(flowFile).value?.trim()
                ?: throw ProcessException("item_id is required")
            val itemId = if (rawItemId.startsWith(TEKST_PREFIX)) rawItemId.removePrefix(TEKST_PREFIX) else rawItemId

            require(isSafeName(itemId)) { "Invalid itemId: $itemId" }

            val bucket = context.getProperty(BUCKET).evaluateAttributeExpressions(flowFile).value
                ?: throw ProcessException("bucket is required")
            val accessKey = context.getProperty(ACCESS_KEY).evaluateAttributeExpressions(flowFile).value
                ?: throw ProcessException("access_key is required")
            val secretKey = context.getProperty(SECRET_KEY).value
                ?: throw ProcessException("secret_key is required")
            val region = context.getProperty(REGION).evaluateAttributeExpressions(flowFile).value
                ?: throw ProcessException("region is required")
            val endpoint = context.getProperty(ENDPOINT).evaluateAttributeExpressions(flowFile).value
                ?: throw ProcessException("endpoint is required")
            val prefix = context.getProperty(PREFIX).evaluateAttributeExpressions(flowFile).value
                ?.trim()
                ?.trim('/')
                .orEmpty()

            val folderName = "$TEKST_PREFIX$itemId"
            // Trailing slash ensures we only match keys under this folder, not e.g. tekst_abc-other/.
            val keyPrefix = if (prefix.isEmpty()) "$folderName/" else "$prefix/$folderName/"

            log.info("Recursively deleting keys with prefix '{}' from bucket '{}'", keyPrefix, bucket)

            val client = getS3Client(accessKey, secretKey, region, endpoint)
            S3Utils.deleteAllKeysWithPrefix(client, bucket, keyPrefix)

            session.transfer(flowFile, REL_SUCCESS)
        } catch (exception: Exception) {
            log.error("Failed to delete MinIO files", exception)
            session.transfer(flowFile, REL_FAILURE)
        }
    }
}