package no.nb.nifi.tekst.processors

import edu.harvard.hul.ois.jhove.App
import edu.harvard.hul.ois.jhove.JhoveBase
import no.nb.nifi.tekst.exceptions.RoutedException
import no.nb.nifi.tekst.validation.XsdValidator
import org.apache.nifi.annotation.behavior.*
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.*
import org.apache.nifi.processor.util.StandardValidators
import java.nio.file.FileSystemException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import javax.xml.namespace.NamespaceContext
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathFactory

@Tags("NB", "Validation", "JHOVE")
@CapabilityDescription(
    ("Validates required files in subfolders of an object folder with JHOVE and writes the JHOVE XML output to the corresponding target folders. " +
            "The processor recursively validates files in defined subfolders, checking all generated XML files to return valid/well-formed statuses. " +
            "Note that to force XML output compliant with MIX10, and hence compatible with CreateMetsBrowsing, " +
            "we've added <mixVersion>1.0</mixVersion> to jhoveconf.xml")
)
@SupportsBatching
class Jhove : AbstractProcessor() {
    private var descriptors: MutableList<PropertyDescriptor> = mutableListOf()
    private var relationships: MutableSet<Relationship> = mutableSetOf()
    private lateinit var configFilePath: String

    companion object {
        /** Application name.  */
        private const val NAME = "Jhove"

        /** Application build date, YYYY, MM, DD.  */
        private val DATE = intArrayOf(2009, 12, 23)

        /** Application release number.  */
        private const val RELEASE = "1.20.1"

        /** Copyright information.  */
        private const val RIGHTS = ("Copyright 2004-2009 by the President and Fellows of Harvard College. "
                + "Released under the GNU Lesser General Public License.")

        private const val WELL_FORMED_AND_VALID = "Well-Formed and valid"

        private const val JHOVE_CONFIG_RESOURCE = "/jhoveconf.xml"

        // Hardcoded subfolder mappings: each key is a source subfolder, value is the target output subfolder for JHOVE XML files
        private val FOLDER_MAPPINGS = mapOf(
			"metadata/descriptive" to "metadata/other/jhove",
            "representations/primary/data" to "representations/primary/metadata/technical/jhove",
            "representations/access/data" to "representations/access/metadata/technical/jhove",
			"representations/access/metadata/other/ocr" to "representations/access/metadata/other/jhove-ocr"
        )

        // File type to JHOVE module mapping
        private val FILE_TYPE_MODULE_MAP = mapOf(
            "jp2" to "JPEG2000-hul",
            "jpeg" to "JPEG-hul",
            "jpg" to "JPEG-hul",
            "tif" to "TIFF-hul",
            "tiff" to "TIFF-hul",
            "png" to "PNG-gdm",
            "gif" to "gif-hul",
            "pdf" to "PDF-hul",
            "xml" to "XML-hul"
        )

        val OBJECT_FOLDER: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("object_folder")
            .displayName("Object Folder")
            .description("Absolute path to the top-level object folder containing subfolders to validate.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build()

        val SUCCESS_RELATIONSHIP: Relationship = Relationship.Builder()
            .name("success")
            .description("All JHOVE validations successful (all files well-formed and valid)")
            .build()

        val WELLFORMED_RELATIONSHIP: Relationship = Relationship.Builder()
            .name("well-formed")
            .description("All files are well-formed, but some are not valid")
            .build()

        val FAIL_RELATIONSHIP: Relationship = Relationship.Builder()
            .name("failure")
            .description("Validation failure")
            .build()

        val EMPTY_RESULT_RELATIONSHIP: Relationship = Relationship.Builder()
            .name("empty")
            .description("No files were found to validate in any of the configured subfolders")
            .build()


        /**
         * Data class to hold validation results for all JHOVE files
         */
        data class ValidationResult(
            val allValid: Boolean,
            val allWellFormed: Boolean,
            val filesProcessed: Int,
            val errors: List<String> = emptyList()
        )
    }

    override fun init(context: ProcessorInitializationContext) {
        descriptors.add(OBJECT_FOLDER)
        descriptors = Collections.unmodifiableList(descriptors)

        relationships = HashSet()
        relationships.add(SUCCESS_RELATIONSHIP)
        relationships.add(WELLFORMED_RELATIONSHIP)
        relationships.add(FAIL_RELATIONSHIP)
        relationships.add(EMPTY_RESULT_RELATIONSHIP)
        relationships = Collections.unmodifiableSet(relationships)

        // Load config from classpath resource and copy to temp file once
        val configStream = javaClass.getResourceAsStream(JHOVE_CONFIG_RESOURCE)
            ?: throw IllegalStateException("Could not load JHOVE config from classpath")
        val tempConfigFile = Files.createTempFile("jhove-config", ".xml")
        tempConfigFile.toFile().deleteOnExit()
        configStream.use { inputStream ->
            Files.copy(inputStream, tempConfigFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
        }
        configFilePath = tempConfigFile.toString()
    }

    override fun getRelationships(): Set<Relationship> {
        return relationships
    }

    override fun getSupportedPropertyDescriptors(): List<PropertyDescriptor> {
        return descriptors
    }

    /**
     * Detects the file type based on file extension and returns the corresponding JHOVE module.
     */
    private fun detectJhoveModule(file: Path): String? {
        val fileName = file.fileName.toString().lowercase()
        val fileExtension = fileName.substringAfterLast(".", "").lowercase()
        return FILE_TYPE_MODULE_MAP[fileExtension]
    }

    /**
     * Represents the status of a single JHOVE validation
     */
    data class FileValidationStatus(
        val filePath: Path,
        val status: String,
        val isValid: Boolean,
        val isWellFormed: Boolean,
        val errorMessage: String? = null
    )

    /**
     * Runs JHOVE validation on a single file and returns the validation status.
     */
    @Throws(RoutedException::class)
    private fun runJhoveOnFile(
        inputFile: Path,
        outputFile: Path,
        moduleName: String,
        configPath: String
    ): FileValidationStatus {
        getLogger().info("Starting Jhove on file $inputFile with module $moduleName")

        try {
            val jhoveApp = App(NAME, RELEASE, DATE, "", RIGHTS)

            val jhoveBase = JhoveBase()
            jhoveBase.init(configPath, null)
            val handler = jhoveBase.getHandler("xml")
            jhoveBase.checksumFlag = true
            jhoveBase.encoding = "UTF-8"
            // Conflict with java.lang.Module
            val module = jhoveBase.getModule(moduleName)
            jhoveBase.dispatch(jhoveApp, module, null, handler, outputFile.toString(), arrayOf(inputFile.toString()))

            // Parse the generated JHOVE XML output to determine status
            val domFactory = DocumentBuilderFactory.newInstance()
            domFactory.isNamespaceAware = true
            val builder = domFactory.newDocumentBuilder()
            val doc = builder.parse(outputFile.toString())
            val factory = XPathFactory.newInstance()
            val xpath = factory.newXPath()
            xpath.namespaceContext = object : NamespaceContext {
                override fun getNamespaceURI(prefix: String): String {
                    return "http://schema.openpreservation.org/ois/xml/ns/jhove"
                }

                override fun getPrefix(namespaceUri: String): String {
                    throw UnsupportedOperationException()
                }

                override fun getPrefixes(namespaceUri: String): MutableIterator<String> {
                    throw UnsupportedOperationException()
                }
            }

            val statusExpr = xpath.compile("/jhove:jhove/jhove:repInfo/jhove:status")
            val status = statusExpr.evaluate(doc) as String

            val isValid = status == WELL_FORMED_AND_VALID
            val isWellFormed = status.contains("Well-Formed")

            val errorMsg = if (!isValid) {
                val msgExpr = xpath.compile("/jhove:jhove/jhove:repInfo/jhove:messages/jhove:message")
                val jhoveMsg = msgExpr.evaluate(doc) as String
                val details = if (jhoveMsg.isBlank()) "No detailed message from JHOVE" else jhoveMsg
                "${inputFile.fileName}: $status ($details)"
            } else {
                null
            }

            return FileValidationStatus(
                filePath = inputFile,
                status = status,
                isValid = isValid,
                isWellFormed = isWellFormed,
                errorMessage = errorMsg
            )

        } catch (e: Exception) {
            throw RoutedException(FAIL_RELATIONSHIP, false, "Exception while running Jhove on $inputFile - " + e.message, e)
        }
    }

    /**
     * Validates all files in the given source folder and writes JHOVE output to the target folder.
     * Returns a list of file validation statuses.
     * Throws on any file processing error — failure on one file fails the entire folder.
     */
    @Throws(RoutedException::class)
    private fun validateFolderContents(
        sourceFolder: Path,
        targetFolder: Path,
        logger: (String) -> Unit
    ): List<FileValidationStatus> {
        val results = mutableListOf<FileValidationStatus>()

        if (!Files.exists(sourceFolder) || !Files.isDirectory(sourceFolder)) {
            logger("Source folder does not exist or is not a directory: $sourceFolder")
            return results
        }

        // Ensure target folder exists — may throw FileSystemException on read-only filesystem
        try {
            Files.createDirectories(targetFolder)
        } catch (e: FileSystemException) {
            throw RoutedException(
                FAIL_RELATIONSHIP,
                false,
                "Cannot create target directory $targetFolder: ${e.message} (${e.reason ?: "Read-only file system?"})",
                e
            )
        }

        // Process all files in the source folder
        val filesToProcess = Files.list(sourceFolder).use { stream ->
            stream.filter { Files.isRegularFile(it) }.toList()
        }

        for (inputFile in filesToProcess) {
            val jhoveModule = detectJhoveModule(inputFile)
            if (jhoveModule == null) {
                logger("Skipping file ${inputFile.fileName} - unsupported file type")
                continue
            }

            val outputFile = targetFolder.resolve("JHOVE_${inputFile.fileName}.xml")
            val status = runJhoveOnFile(inputFile, outputFile, jhoveModule, configFilePath)

			// Validate JHOVE output against XSD
            val jhoveContent = Files.readString(outputFile)
            val validationResult = XsdValidator.validateJhove(jhoveContent)
            if (!validationResult.isValid) {
                val xsdError = "${inputFile.fileName}: XSD validation failed: ${validationResult.getErrorMessage()}"
                logger("JHOVE output failed XSD validation for ${inputFile.fileName}: ${validationResult.getErrorMessage()}")
                results.add(
                    status.copy(
                        errorMessage = if (status.errorMessage.isNullOrBlank()) xsdError else "${status.errorMessage} | $xsdError"
                    )
                )
            } else {
                results.add(status)
                logger("JHOVE validation successful for ${inputFile.fileName}: ${status.status}")
            }
        }

        return results
    }

    override fun onTrigger(context: ProcessContext, session: ProcessSession) {
        var flowFile = session.get() ?: return

        try {
            val objectFolderPath = context.getProperty(OBJECT_FOLDER)
                .evaluateAttributeExpressions(flowFile).value

            if (objectFolderPath.isNullOrBlank()) {
                throw RoutedException(
                    FAIL_RELATIONSHIP,
                    message = "Object Folder property is not configured"
                )
            }

            val objectFolder = Paths.get(objectFolderPath)
            if (!Files.exists(objectFolder) || !Files.isDirectory(objectFolder)) {
                throw RoutedException(
                    FAIL_RELATIONSHIP,
                    message = "Object folder does not exist: $objectFolderPath"
                )
            }

            val allValidationResults = mutableListOf<FileValidationStatus>()

            // Process each configured folder mapping
            for ((sourceSubfolder, targetSubfolder) in FOLDER_MAPPINGS) {
                val sourcePath = objectFolder.resolve(sourceSubfolder)
                val targetPath = objectFolder.resolve(targetSubfolder)

                val results = validateFolderContents(
                    sourcePath,
                    targetPath,
                    { msg -> getLogger().info(msg) }
                )

                allValidationResults.addAll(results)
            }

            // Aggregate validation results
            if (allValidationResults.isEmpty()) {
                getLogger().info("No files found to validate in any subfolder - routing to empty")
                flowFile = session.putAttribute(flowFile, "jhove.files_processed", "0")
                session.transfer(flowFile, EMPTY_RESULT_RELATIONSHIP)
                return
            }

            val allValid = allValidationResults.all { it.isValid }
            val allWellFormed = allValidationResults.all { it.isWellFormed }
            val errorList = allValidationResults
                .mapNotNull { it.errorMessage }

            val validationSummary = ValidationResult(
                allValid = allValid,
                allWellFormed = allWellFormed,
                filesProcessed = allValidationResults.size,
                errors = errorList
            )

            // Add validation summary to flowfile attributes
            flowFile = session.putAttribute(flowFile, "jhove.files_processed", validationSummary.filesProcessed.toString())
            flowFile = session.putAttribute(flowFile, "jhove.all_valid", validationSummary.allValid.toString())
            flowFile = session.putAttribute(flowFile, "jhove.all_wellformed", validationSummary.allWellFormed.toString())

            if (validationSummary.errors.isNotEmpty()) {
                flowFile = session.putAttribute(flowFile, "jhove.errors", validationSummary.errors.joinToString("; "))
            }

            // Route based on validation results
            when {
                validationSummary.allValid -> {
                    getLogger().info("All JHOVE validations successful - routing to success")
                    session.transfer(flowFile, SUCCESS_RELATIONSHIP)
                }
                validationSummary.allWellFormed -> {
                    getLogger().info("All files are well-formed but some are not valid - routing to well-formed")
                    session.transfer(flowFile, WELLFORMED_RELATIONSHIP)
                }
                else -> {
                    throw RoutedException(
                        FAIL_RELATIONSHIP,
                        false,
                        "JHOVE validation failed: ${validationSummary.errors.joinToString("; ")}",
                        null
                    )
                }
            }

        } catch (routed: RoutedException) {
            if (routed.penalize == true) {
                flowFile = session.penalize(flowFile)
            }
            val errorMsg = if (routed.cause != null) {
                "${routed.message}: ${routed.cause}"
            } else {
                routed.message ?: "Unknown error"
            }
            flowFile = session.putAttribute(flowFile, "error.message", errorMsg)
            session.transfer(flowFile, routed.relationship)
        } catch (e: Exception) {
            getLogger().error("Unexpected error in JHOVE processor", e)
            flowFile = session.putAttribute(flowFile, "error.message", "Unexpected error: ${e.message}")
            session.transfer(flowFile, FAIL_RELATIONSHIP)
        }
    }
}
