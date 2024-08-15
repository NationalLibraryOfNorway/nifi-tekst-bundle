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
package no.nb.processors.tekst;

import io.minio.DownloadObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Tags({"NB", "Tekst", "Text", "S3", "Download"})
@CapabilityDescription("This processor downloads all files starting with prefix (folder-like) in an S3 bucket. " +
                       "Works for any regions (which the default S3 processors does not), " +
                       "meaning it should work for all S3 compatible storages. " +
                       "Local folder is created if it does not exist.")
@SideEffectFree
public class DownloadMultipleS3FilesByPrefix extends AbstractProcessor {

    public static final PropertyDescriptor BUCKET = new PropertyDescriptor
        .Builder()
        .name("bucket")
        .displayName("S3 bucket")
        .description("S3 bucket name")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor ACCESS_KEY = new PropertyDescriptor
        .Builder()
        .name("access_key")
        .displayName("S3 access key")
        .description("S3 access key")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor SECRET_KEY = new PropertyDescriptor
        .Builder()
        .name("secret_key")
        .displayName("S3 secret key")
        .description("S3 secret key")
        .required(true)
        .sensitive(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor REGION = new PropertyDescriptor
        .Builder()
        .name("region")
        .displayName("S3 region")
        .description("S3 region")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor PREFIX = new PropertyDescriptor
        .Builder()
        .name("prefix")
        .displayName("Prefix")
        .description("Prefix (folder-like) in S3 that contains the files to download")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor
        .Builder()
        .name("endpoint")
        .displayName("Endpoint")
        .description("S3 endpoint (url)")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor LOCAL_FOLDER = new PropertyDescriptor
        .Builder()
        .name("local_folder")
        .displayName("Local folder")
        .description("Local folder on server to download the files to")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final Relationship REL_FAILURE = new Relationship
        .Builder()
        .description("Failed processing")
        .name("failure")
        .build();


    public static final Relationship REL_SUCCESS = new Relationship
        .Builder()
        .description("Succeed processing")
        .name("success")
        .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private MinioClient client;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(BUCKET);
        descriptors.add(ACCESS_KEY);
        descriptors.add(SECRET_KEY);
        descriptors.add(REGION);
        descriptors.add(PREFIX);
        descriptors.add(LOCAL_FOLDER);
        descriptors.add(ENDPOINT);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Extract properties
        String bucket = context.getProperty("bucket").evaluateAttributeExpressions(flowFile).getValue();
        String accessKey = context.getProperty("access_key").evaluateAttributeExpressions(flowFile).getValue();
        String secretKey = context.getProperty("secret_key").getValue();
        String region = context.getProperty("region").evaluateAttributeExpressions(flowFile).getValue();
        String endpoint = context.getProperty("endpoint").evaluateAttributeExpressions(flowFile).getValue();
        String prefix = context.getProperty("prefix").evaluateAttributeExpressions(flowFile).getValue();
        String localFolder = context.getProperty("local_folder").evaluateAttributeExpressions(flowFile).getValue();

        client = getS3Client(accessKey, secretKey, region, endpoint);
        downloadAllItems(bucket, prefix, localFolder);

        session.transfer(flowFile, REL_SUCCESS);
    }

    private MinioClient getS3Client(
        String accessKey,
        String secretKey,
        String region,
        String url
    ) {
        return MinioClient
            .builder()
            .endpoint(url)
            .region(region)
            .credentials(accessKey, secretKey)
            .build();
    }

    private String addTrailingSlashIfNotPresent(String str) {
        if (!str.endsWith("/")) {
            return str + "/";
        }
        return str;
    }

    private Iterable<Result<Item>> listItemsByPrefix(
        String bucket,
        String prefix
    ) {
        return client.listObjects(
            ListObjectsArgs
                .builder()
                .bucket(bucket)
                .prefix(prefix)
                .build()
        );
    }

    private void downloadAllItems(
        String bucket,
        String prefix,
        String localFolder
    ) {
        Iterable<Result<Item>> items = listItemsByPrefix(bucket, addTrailingSlashIfNotPresent(prefix));

        // Create directory if it does not exist - nio method does not throw if it exists
        try {
            Path path = Paths.get(localFolder);
            Files.createDirectories(path);
            getLogger().info("Downloading to " + localFolder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        items.forEach(mainItem -> {
            try {
                String objectName = mainItem.get().objectName();
                getLogger().info("Downloading file " + objectName);

                String[] objNameParts = objectName.split("/");
                String fileName = objNameParts[objNameParts.length - 1];

                client.downloadObject(
                    DownloadObjectArgs
                        .builder()
                        .bucket(bucket)
                        .object(objectName)
                        .filename(localFolder + "/" + fileName)
                        .build()
                );

                getLogger().info("Finished downloaded file " + objectName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
