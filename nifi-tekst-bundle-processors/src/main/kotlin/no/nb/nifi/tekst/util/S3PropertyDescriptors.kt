package no.nb.nifi.tekst.util

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.util.StandardValidators

/**
 * Shared NiFi [PropertyDescriptor]s for S3/MinIO connection settings.
 *
 * Centralizes the bucket / access key / secret key / region / endpoint properties
 * so processors that talk to the same S3 bucket present a consistent UI and
 * validation surface to flow authors.
 */
object S3PropertyDescriptors {

    val BUCKET: PropertyDescriptor = PropertyDescriptor.Builder()
        .name("bucket")
        .displayName("S3 bucket")
        .description("S3 bucket name")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build()

    val ACCESS_KEY: PropertyDescriptor = PropertyDescriptor.Builder()
        .name("access_key")
        .displayName("S3 access key")
        .description("S3 access key")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build()

    val ENDPOINT: PropertyDescriptor = PropertyDescriptor.Builder()
        .name("endpoint")
        .displayName("Endpoint")
        .description("S3 endpoint (url)")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build()
}

