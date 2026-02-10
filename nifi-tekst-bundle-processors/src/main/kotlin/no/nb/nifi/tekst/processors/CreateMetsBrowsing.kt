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

import no.nb.nifi.tekst.mets.*
import no.nb.nifi.tekst.validation.XsdValidator
import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.*
import org.apache.nifi.processor.util.StandardValidators
import java.io.File
import java.security.MessageDigest
import java.util.*
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathFactory

@Tags("NB", "Tekst", "Text", "METS", "Browsing", "Create")
@CapabilityDescription(
    ("Creates METS-browsing.xml for object")
)
@SideEffectFree
class CreateMetsBrowsing : AbstractProcessor() {
    private var descriptors: MutableList<PropertyDescriptor> = mutableListOf()

    private var relationships: MutableSet<Relationship> = mutableSetOf()

    companion object {
        val OBJECT_FOLDER: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("object_folder")
            .displayName("Object Folder")
            .description("Absolute path to the object folder. The folder name (last path component) is used as the Object ID. " +
                    "Example: /data/objects/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val ALTO_FOLDER: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("alto_folder")
            .displayName("ALTO Folder")
            .description("Path to ALTO XML files. Can be relative to the Object Folder (e.g., access/metadata/other/ocr) or absolute.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("access/metadata/other/ocr")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val IMAGE_FOLDER: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("image_folder")
            .displayName("Image Folder")
            .description("Path to JP2 image files. Can be relative to the Object Folder (e.g., access/data) or absolute.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("access/data")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val JHOVE_FOLDER: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("jhove_folder")
            .displayName("JHOVE Folder")
            .description("Path to JHOVE XML metadata files. Can be relative to the Object Folder (e.g., access/metadata/technical/jhove) or absolute.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("access/metadata/technical/jhove")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val OUTPUT_FILE: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("output_file")
            .displayName("Output File")
            .description("Path for the output METS-browsing XML file. Can be relative to the Object Folder " +
                    "(e.g., access/metadata/METS-browsing.xml) or an absolute path.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("METS-browsing.xml")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val AGENT_NAME: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("agent_name")
            .displayName("Agent Name")
            .description("Name of the software agent creating the METS file")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .defaultValue("nifi-tekst-bundle")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val METS_VERSION: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("mets_version")
            .displayName("METS Version")
            .description("METS schema version to use (METS_1 or METS_2). Default is METS_2.")
            .required(false)
            .defaultValue("METS_2")
            .allowableValues("METS_1", "METS_2")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build()

        val MIX_VERSION: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("mix_version")
            .displayName("MIX Version")
            .description("MIX (Metadata for Images in XML) schema version to use (MIX_1_0 or MIX_2_0). Default is MIX_2_0.")
            .required(false)
            .defaultValue("MIX_2_0")
            .allowableValues("MIX_1_0", "MIX_2_0")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
        descriptors.add(OBJECT_FOLDER)
        descriptors.add(ALTO_FOLDER)
        descriptors.add(IMAGE_FOLDER)
        descriptors.add(JHOVE_FOLDER)
        descriptors.add(OUTPUT_FILE)
        descriptors.add(AGENT_NAME)
        descriptors.add(METS_VERSION)
        descriptors.add(MIX_VERSION)
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

        try {
            // Extract properties
            val objectFolderPath = context.getProperty(OBJECT_FOLDER).evaluateAttributeExpressions(flowFile).value
            val altoFolderRelative = context.getProperty(ALTO_FOLDER).evaluateAttributeExpressions(flowFile).value
            val imageFolderRelative = context.getProperty(IMAGE_FOLDER).evaluateAttributeExpressions(flowFile).value
            val jhoveFolderRelative = context.getProperty(JHOVE_FOLDER).evaluateAttributeExpressions(flowFile).value
            val outputFileRelative = context.getProperty(OUTPUT_FILE).evaluateAttributeExpressions(flowFile).value
            val agentName = context.getProperty(AGENT_NAME).evaluateAttributeExpressions(flowFile).value ?: "nifi-tekst-bundle"
            val metsVersionStr = context.getProperty(METS_VERSION).value ?: "METS_2"
            val mixVersionStr = context.getProperty(MIX_VERSION).value ?: "MIX_2_0"

            // Convert version strings to enums
            val metsVersion = MetsVersion.valueOf(metsVersionStr)
            val mixVersion = MixVersion.valueOf(mixVersionStr)

            // Resolve object folder and extract Object ID from folder name
            val objectFolder = File(objectFolderPath)
            val objId = objectFolder.name

            if (!objectFolder.exists() || !objectFolder.isDirectory) {
                logger.error("Object folder does not exist: $objectFolderPath")
                session.transfer(flowFile, REL_FAILURE)
                return
            }

            // Resolve paths - can be absolute or relative to object folder
            val altoFolderSpec = File(altoFolderRelative)
            val altoFolder = if (altoFolderSpec.isAbsolute) altoFolderSpec else File(objectFolder, altoFolderRelative)

            val imageFolderSpec = File(imageFolderRelative)
            val imageFolder = if (imageFolderSpec.isAbsolute) imageFolderSpec else File(objectFolder, imageFolderRelative)

            val jhoveFolderSpec = File(jhoveFolderRelative)
            val jhoveFolder = if (jhoveFolderSpec.isAbsolute) jhoveFolderSpec else File(objectFolder, jhoveFolderRelative)

            val outputFileSpec = File(outputFileRelative)
            val outputFile = if (outputFileSpec.isAbsolute) outputFileSpec else File(objectFolder, outputFileRelative)

            // Validate folders exist
            if (!altoFolder.exists() || !altoFolder.isDirectory) {
                logger.error("ALTO folder does not exist: ${altoFolder.absolutePath}")
                session.transfer(flowFile, REL_FAILURE)
                return
            }

            if (!imageFolder.exists() || !imageFolder.isDirectory) {
                logger.error("Image folder does not exist: ${imageFolder.absolutePath}")
                session.transfer(flowFile, REL_FAILURE)
                return
            }

            if (!jhoveFolder.exists() || !jhoveFolder.isDirectory) {
                logger.error("JHOVE folder does not exist: ${jhoveFolder.absolutePath}")
                session.transfer(flowFile, REL_FAILURE)
                return
            }

            // Get sorted lists of files
            val altoFiles = altoFolder.listFiles { file -> file.extension == "xml" }
                ?.sortedBy { it.name } ?: emptyList()

            val imageFiles = imageFolder.listFiles { file -> file.extension == "jp2" }
                ?.sortedBy { it.name } ?: emptyList()

            if (altoFiles.isEmpty()) {
                logger.error("No ALTO XML files found in: ${altoFolder.absolutePath}")
                session.transfer(flowFile, REL_FAILURE)
                return
            }

            if (imageFiles.isEmpty()) {
                logger.error("No JP2 image files found in: ${imageFolder.absolutePath}")
                session.transfer(flowFile, REL_FAILURE)
                return
            }

            if (altoFiles.size != imageFiles.size) {
                logger.warn("Number of ALTO files (${altoFiles.size}) does not match number of image files (${imageFiles.size})")
            }

            // Build page info for each page
            val pages = altoFiles.zip(imageFiles).mapIndexed { index, (altoFile, imageFile) ->
                val pageNumber = index + 1

                // Extract base name from files (without extension)
                val imageBaseName = imageFile.nameWithoutExtension
                val altoBaseName = altoFile.nameWithoutExtension

                // Find corresponding JHOVE file for this image
                val jhoveFile = findJhoveFileForImage(jhoveFolder, imageFile)
                    ?: throw IllegalStateException(
                        "Missing JHOVE file for image '${imageFile.name}'. " +
                        "Expected JHOVE file at: ${File(jhoveFolder, "JHOVE_${imageFile.name}.xml").absolutePath}. " +
                        "Please ensure all images have been processed through the Jhove processor before generating METS-browsing."
                    )
                val resolution = extractImageDimensionsFromJhove(jhoveFile)

                PageInfo(
                    pageNumber = pageNumber,
                    altoFile = AltoFileInfo(
                        size = altoFile.length(),
                        checksum = calculateMd5(altoFile),
                        urn = "URN:NBN:no-nb_${altoBaseName}",
                        url = "file://ocr/${altoFile.name}"
                    ),
                    imageFile = ImageFileInfo(
                        size = imageFile.length(),
                        checksum = calculateMd5(imageFile),
                        urn = "URN:NBN:no-nb_${imageBaseName}",
                        url = "file://ocr/${imageFile.name}"
                    ),
                    resolution = resolution
                )
            }

            // Generate METS-browsing XML
            val generator = MetsBrowsingGenerator()
            val metsXml = generator.generateMetsBrowsing(
                objId = objId,
                pages = pages,
                agentName = agentName,
                version = metsVersion,
                mixVersion = mixVersion
            )

            // Validate generated XML against XSD before writing
            val validationResult = when (metsVersion) {
                MetsVersion.METS_1 -> XsdValidator.validateMets(metsXml)
                MetsVersion.METS_2 -> XsdValidator.validateMets2(metsXml)
            }
            if (!validationResult.isValid) {
                logger.error("Generated METS-browsing XML failed XSD validation: ${validationResult.getErrorMessage()}")
                session.transfer(flowFile, REL_FAILURE)
                return
            }

            // Write validated XML to output file
            outputFile.writeText(metsXml)

            logger.info("Successfully generated METS-browsing: ${outputFile.absolutePath}")
            session.transfer(flowFile, REL_SUCCESS)

        } catch (e: Exception) {
            logger.error("Failed to generate METS-browsing", e)
            session.transfer(flowFile, REL_FAILURE)
        }
    }

    private fun calculateMd5(file: File): String {
        val md = MessageDigest.getInstance("MD5")
        file.inputStream().use { input ->
            val buffer = ByteArray(8192)
            var bytesRead: Int
            while (input.read(buffer).also { bytesRead = it } != -1) {
                md.update(buffer, 0, bytesRead)
            }
        }
        return md.digest().joinToString("") { "%02x".format(it) }
    }

    private fun findJhoveFileForImage(jhoveFolder: File, imageFile: File): File? {
        // JHOVE files are typically named: JHOVE_<imagename>.xml
        // e.g., romsdalsposten_null_null_18970814_22_96_1-1_001_null.jp2 -> JHOVE_romsdalsposten_null_null_18970814_22_96_1-1_001_null.jp2.xml
        val imageName = imageFile.name
        val jhoveFileName = "JHOVE_$imageName.xml"
        val jhoveFile = File(jhoveFolder, jhoveFileName)

        return if (jhoveFile.exists()) {
            jhoveFile
        } else {
            null
        }
    }

    private fun extractImageDimensionsFromJhove(jhoveFile: File): ImageResolution {
        try {
            val factory = DocumentBuilderFactory.newInstance()
            factory.isNamespaceAware = true
            val builder = factory.newDocumentBuilder()
            val doc = builder.parse(jhoveFile)

            val xPath = XPathFactory.newInstance().newXPath()

            // Extract imageWidth and imageHeight from mix namespace
            val widthExpr = "//*[local-name()='imageWidth']/text()"
            val heightExpr = "//*[local-name()='imageHeight']/text()"

            val width = xPath.evaluate(widthExpr, doc)?.toIntOrNull() ?: 2127
            val height = xPath.evaluate(heightExpr, doc)?.toIntOrNull() ?: 3387

            return ImageResolution(width = width, height = height)
        } catch (e: Exception) {
            logger.error("Failed to parse JHOVE file: ${jhoveFile.absolutePath}", e)
            return ImageResolution(width = 2127, height = 3387)
        }
    }

}