/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nifi.tekst.processors

import io.minio.DownloadObjectArgs
import io.minio.ListObjectsArgs
import io.minio.MinioClient
import io.minio.messages.Item
import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.*
import org.apache.nifi.processor.util.StandardValidators
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

@Tags("NB", "Tekst", "Text", "S3", "Download")
@CapabilityDescription(
    ("This processor downloads all files starting with prefix (folder-like) in an S3 bucket. " +
            "Works for any regions (which the default S3 processors does not), " +
            "meaning it should work for all S3 compatible storages. " +
            "Local folder is created if it does not exist.")
)
@SideEffectFree
class DownloadMultipleS3FilesByPrefix : AbstractProcessor() {
    private var descriptors: MutableList<PropertyDescriptor> = mutableListOf()

    private var relationships: MutableSet<Relationship> = mutableSetOf()

    private lateinit var client: MinioClient

    companion object {
        val BUCKET: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("bucket")
            .displayName("S3 bucket")
            .description("S3 bucket name")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val ACCESS_KEY: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("access_key")
            .displayName("S3 access key")
            .description("S3 access key")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val SECRET_KEY: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("secret_key")
            .displayName("S3 secret key")
            .description("S3 secret key")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val REGION: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("region")
            .displayName("S3 region")
            .description("S3 region")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val PREFIX: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("prefix")
            .displayName("Prefix")
            .description("Prefix (folder-like) in S3 that contains the files to download")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val ENDPOINT: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("endpoint")
            .displayName("Endpoint")
            .description("S3 endpoint (url)")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val LOCAL_FOLDER: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("local_folder")
            .displayName("Local folder")
            .description("Local folder on server to download the files to")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val REL_FAILURE: Relationship = Relationship.Builder()
            .description("Failed processing")
            .name("failure")
            .build()


        val REL_SUCCESS: Relationship = Relationship.Builder()
            .description("Succeed processing")
            .name("success")
            .build()
    }

    override fun init(context: ProcessorInitializationContext) {
        descriptors.add(BUCKET)
        descriptors.add(ACCESS_KEY)
        descriptors.add(SECRET_KEY)
        descriptors.add(REGION)
        descriptors.add(PREFIX)
        descriptors.add(LOCAL_FOLDER)
        descriptors.add(ENDPOINT)
        descriptors = Collections.unmodifiableList(descriptors)

        relationships = HashSet()
        relationships.add(REL_FAILURE)
        relationships.add(REL_SUCCESS)
        relationships = Collections.unmodifiableSet(relationships)
    }

    override fun getRelationships(): Set<Relationship> {
        return relationships
    }

    public override fun getSupportedPropertyDescriptors(): List<PropertyDescriptor> {
        return descriptors
    }

    override fun onTrigger(context: ProcessContext, session: ProcessSession) {
        val flowFile = session.get() ?: return

        // Extract properties
        val bucket = context.getProperty("bucket").evaluateAttributeExpressions(flowFile).value
        val accessKey = context.getProperty("access_key").evaluateAttributeExpressions(flowFile).value
        val secretKey = context.getProperty("secret_key").value
        val region = context.getProperty("region").evaluateAttributeExpressions(flowFile).value
        val endpoint = context.getProperty("endpoint").evaluateAttributeExpressions(flowFile).value
        val prefix = context.getProperty("prefix").evaluateAttributeExpressions(flowFile).value
        val localFolder = context.getProperty("local_folder").evaluateAttributeExpressions(flowFile).value

        client = getS3Client(accessKey, secretKey, region, endpoint)

        try {
            downloadAllItems(bucket, prefix, localFolder)
        } catch (e: Exception) {
            logger.error("Failed to download files", e)
            session.transfer(flowFile, REL_FAILURE)
            return
        }

        session.transfer(flowFile, REL_SUCCESS)
    }

    private fun getS3Client(
        accessKey: String,
        secretKey: String,
        region: String,
        url: String
    ): MinioClient {
        return MinioClient
            .builder()
            .endpoint(url)
            .region(region)
            .credentials(accessKey, secretKey)
            .build()
    }

    private fun addTrailingSlashIfNotPresent(str: String): String {
        if (!str.endsWith("/")) {
            return "$str/"
        }
        return str
    }

    private fun listItemsByPrefix(
        bucket: String,
        prefix: String
    ): MutableIterable<io.minio.Result<Item>>? {
        return client.listObjects(
            ListObjectsArgs
                .builder()
                .bucket(bucket)
                .prefix(prefix)
                .build()
        )
    }

    private fun downloadAllItems(
        bucket: String,
        prefix: String,
        localFolder: String
    ) {
        val items: MutableIterable<io.minio.Result<Item>>? = listItemsByPrefix(bucket, addTrailingSlashIfNotPresent(prefix))
        if (items == null || !items.iterator().hasNext()) {
            logger.error("No items found in bucket $bucket with prefix $prefix")
            throw RuntimeException("No items found in bucket $bucket with prefix $prefix")
        }

        // Create directory if it does not exist - nio method does not throw if it exists
        try {
            val path = Paths.get(localFolder)
            Files.createDirectories(path)
            logger.info("Downloading to $localFolder")
        } catch (e: IOException) {
            throw RuntimeException(e)
        }

        items.forEach { mainItem: io.minio.Result<Item> ->
            try {
                val objectName: String = mainItem.get().objectName()
                logger.info("Downloading file $objectName")

                val objNameParts = objectName.split("/".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                val fileName = objNameParts[objNameParts.size - 1]

                client.downloadObject(
                    DownloadObjectArgs
                        .builder()
                        .bucket(bucket)
                        .`object`(objectName)
                        .filename("$localFolder/$fileName")
                        .build()
                )

                logger.info("Finished downloaded file $objectName")
            } catch (e: Exception) {
                throw RuntimeException(e)
            }
        }
    }
}