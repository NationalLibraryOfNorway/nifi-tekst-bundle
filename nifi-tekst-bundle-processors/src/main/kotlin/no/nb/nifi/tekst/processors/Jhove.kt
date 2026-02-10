package no.nb.nifi.tekst.processors

import edu.harvard.hul.ois.jhove.App
import edu.harvard.hul.ois.jhove.JhoveBase
import no.nb.nifi.tekst.exceptions.RoutedException
import no.nb.nifi.tekst.util.NiFiAttributes
import no.nb.nifi.tekst.validation.XsdValidator
import org.apache.nifi.annotation.behavior.*
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.AllowableValue
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.*
import org.apache.nifi.processor.util.StandardValidators
import java.io.FileInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import javax.xml.namespace.NamespaceContext
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathFactory

@Tags("NB", "Validation", "JHOVE")
@CapabilityDescription(
    ("Validates a file with JHOVE and writes the JHOVE XML output to disk. " +
            "The output file is written to the configured 'Output folder path' with the naming pattern 'JHOVE_<filename>.xml'. " +
            "Note that to force XML output compliant with MIX10, and hence compatible with CreateMetsBrowsing, " +
            "we've added <mixVersion>1.0</mixVersion> to jhoveconf.xml")
)
@ReadsAttributes(ReadsAttribute(attribute = NiFiAttributes.FILENAME, description = ""))
@WritesAttributes(
    WritesAttribute(attribute = NiFiAttributes.FILENAME, description = ""),
    WritesAttribute(attribute = NiFiAttributes.MIME_TYPE, description = ""),
    WritesAttribute(attribute = NiFiAttributes.FILE_SIZE, description = ""),
)
@SideEffectFree
@SupportsBatching
class Jhove : AbstractProcessor() {
    private var errorMessage: String? = null
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
        private const val WELL_FORMED_NOT_VALID = "Well-Formed, but not valid"
        private const val WELL_FORMED = "Well-Formed"

        // JHOVE Module allowable values
        val MODULE_AIFF = AllowableValue("AIFF-hul", "AIFF", "Audio Interchange File Format")
        val MODULE_ASCII = AllowableValue("ASCII-hul", "ASCII", "ASCII text")
        val MODULE_GIF = AllowableValue("gif-hul", "GIF", "Graphics Interchange Format")
        val MODULE_HTML = AllowableValue("HTML-hul", "HTML", "HyperText Markup Language")
        val MODULE_JPEG = AllowableValue("JPEG-hul", "JPEG", "JPEG image")
        val MODULE_JPEG2000 = AllowableValue("JPEG2000-hul", "JPEG2000", "JPEG 2000 image")
        val MODULE_PDF = AllowableValue("PDF-hul", "PDF", "Portable Document Format")
        val MODULE_TIFF = AllowableValue("TIFF-hul", "TIFF", "Tagged Image File Format")
        val MODULE_UTF8 = AllowableValue("UTF8-hul", "UTF-8", "UTF-8 encoded text")
        val MODULE_XML = AllowableValue("XML-hul", "XML", "Extensible Markup Language")
        val MODULE_PNG = AllowableValue("PNG-gdm", "PNG", "Portable Network Graphics")

        val MODULE: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("JHOVE module")
            .description("The JHOVE module to use for file validation")
            .required(true)
            .allowableValues(
                MODULE_AIFF, MODULE_ASCII, MODULE_GIF, MODULE_HTML, MODULE_JPEG,
                MODULE_JPEG2000, MODULE_PDF, MODULE_TIFF, MODULE_UTF8, MODULE_XML, MODULE_PNG
            )
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build()

        private const val JHOVE_CONFIG_RESOURCE = "/jhoveconf.xml"

        val OUTPUT_PATH: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("Output folder path")
            .description("Path to folder where JHOVE XML output files will be written to disk (e.g., /data/jhove-output). Files are named 'JHOVE_<filename>.xml'.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build()

        val INPUT_PATH: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("Input folder path")
            .description("Path to folder containing files to analyse.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build()

        val CONTINUE_ON_ERROR: AllowableValue = AllowableValue(
            "continue",
            "Continue on error",
            "Route XML output to success regardless of JHOVE status"
        )

        val FAIL_ON_ERROR: AllowableValue = AllowableValue(
            "fail",
            "Fail on error",
            "Route the flowfile to failure if JHOVE reports error"
        )

        val BEHAVIOUR_ON_ERROR: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("Behaviour on JHOVE error")
            .description("Defines how the processor behves if JHOVE reports status other than valid and well-formed")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .allowableValues(CONTINUE_ON_ERROR, FAIL_ON_ERROR)
            .defaultValue("fail")
            .build()

        val SUCCESS_RELATIONSHIP: Relationship = Relationship.Builder()
            .name("success")
            .description("The file is well-formed and valid")
            .build()

        val WELLFORMED_RELATIONSHIP: Relationship = Relationship.Builder()
            .name("Well-formed but not valid")
            .description("The file is well-formed, but not valid")
            .build()

        val FAIL_RELATIONSHIP: Relationship = Relationship.Builder()
            .name("failure")
            .description("Validation failure")
            .build()

        val JHOVE_OUTPUT_RELATIONSHIP: Relationship = Relationship.Builder()
            .name("jhove xml output")
            .description("FlowFile containing the JHOVE XML output (also written to disk at the configured output path)")
            .build()
    }

    override fun init(context: ProcessorInitializationContext) {
        descriptors.add(INPUT_PATH)
        descriptors.add(OUTPUT_PATH)
        descriptors.add(MODULE)
        descriptors.add(BEHAVIOUR_ON_ERROR)
        descriptors = Collections.unmodifiableList(descriptors)

        relationships = HashSet()
        relationships.add(SUCCESS_RELATIONSHIP)
        relationships.add(WELLFORMED_RELATIONSHIP)
        relationships.add(FAIL_RELATIONSHIP)
        relationships.add(JHOVE_OUTPUT_RELATIONSHIP)
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

    @Throws(RoutedException::class)
    private fun runJhove(
        inputFile: Path,
        outputFile: Path,
        moduleName: String,
        configPath: String,
        errorMode: String
    ): Int {
        getLogger().info("Starting Jhove on file $inputFile")

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

            // Må sjekke den genererte outputfila for å finne ut om JHOVE ga noen feilstatus.
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

                override fun getPrefixes(namespaceUri: String): MutableIterator<String>? {
                    throw UnsupportedOperationException()
                }
            }

            // Les status, og sammenlign med kjent OK-status, hvis vi ikke matcher, må vi avslutte med feil.
            var expr = xpath.compile("/jhove:jhove/jhove:repInfo/jhove:status")
            val status = expr.evaluate(doc)
            if (status == WELL_FORMED_AND_VALID) {
                return 0
            } else if (status == WELL_FORMED_NOT_VALID || status == WELL_FORMED) {
                expr = xpath.compile("/jhove:jhove/jhove:repInfo/jhove:messages/jhove:message")
                errorMessage = expr.evaluate(doc)
                return 1
            } else {
                if (errorMode != "fail") {
                    return 0
                } else {
                    expr = xpath.compile("/jhove:jhove/jhove:repInfo/jhove:messages/jhove:message")
                    errorMessage = expr.evaluate(doc)
                    return 2
                }
            }
        } catch (e: Exception) {
            throw RoutedException(FAIL_RELATIONSHIP, false, "Exception while running Jhove - " + e.message, e)
        }
    }

    override fun onTrigger(context: ProcessContext, session: ProcessSession) {
        var flowFile = session.get() ?: return

        val outputPath = context.getProperty(OUTPUT_PATH).evaluateAttributeExpressions(flowFile).value
        val inputPath = context.getProperty(INPUT_PATH).evaluateAttributeExpressions(flowFile).value
        val module = context.getProperty(MODULE).evaluateAttributeExpressions(flowFile).value

        try {
            val filename: String = flowFile.attributes[NiFiAttributes.FILENAME] ?: throw RoutedException(
                FAIL_RELATIONSHIP,
                message = "Filename attribute is missing in flowfile"
            )
            val errorMode = context.getProperty(BEHAVIOUR_ON_ERROR).value

            if (module == null || module == "") {
                session.transfer(flowFile, SUCCESS_RELATIONSHIP)
                return
            }

            val inputFile = Paths.get(inputPath).resolve(filename)
            val outputFile = Paths.get(outputPath).resolve("JHOVE_$filename.xml")

            val res = runJhove(inputFile, outputFile, module, configFilePath, errorMode)

            if (Files.exists(outputFile)) {
                    // Validate JHOVE output against XSD before processing
                    val jhoveContent = Files.readString(outputFile)
                    val validationResult = XsdValidator.validateJhove(jhoveContent)
                    if (!validationResult.isValid) {
                        throw RoutedException(
                            FAIL_RELATIONSHIP,
                            false,
                            "JHOVE output failed XSD validation: ${validationResult.getErrorMessage()}",
                            null
                        )
                    }

                    var outputFlowFile = session.create(flowFile)
                    outputFlowFile =
                        session.putAttribute(outputFlowFile, NiFiAttributes.FILENAME, outputFile.fileName.toString())
                    outputFlowFile = session.putAttribute(outputFlowFile, NiFiAttributes.MIME_TYPE, "application/xml")
                    outputFlowFile = session.putAttribute(
                        outputFlowFile,
                        NiFiAttributes.FILE_SIZE,
                        Files.size(outputFile).toString()
                    )

                    try {
                        FileInputStream(outputFile.toString()).use { fis ->
                            outputFlowFile = session.importFrom(fis, outputFlowFile)
                            session.provenanceReporter.create(outputFlowFile, "Produced JHOVE XML output")
                            session.transfer(outputFlowFile, JHOVE_OUTPUT_RELATIONSHIP)
                            getLogger().info("Jhove OK for file $filename")
                            if (res == 0) {
                                session.transfer(flowFile, SUCCESS_RELATIONSHIP)
                            } else if (res == 1) {
                                flowFile = session.putAttribute(
                                    flowFile, "error.message", "Jhove message: well-formed " +
                                            "but not valid, " + errorMessage
                                )
                                session.transfer(flowFile, WELLFORMED_RELATIONSHIP)
                            } else {
                                throw RoutedException(
                                    FAIL_RELATIONSHIP,
                                    false,
                                    "Jhove failed on file $inputFile: $errorMessage",
                                    null
                                )
                            }
                        }
                    } catch (ioe: IOException) {
                        val msg = String.format(
                            "Could not fetch file %s from file system due to %s; routing to failure",
                            outputFile, ioe.toString()
                        )
                        throw RoutedException(FAIL_RELATIONSHIP, false, msg, ioe)
                    }
                } else {
                    throw RoutedException(FAIL_RELATIONSHIP, false, "Output file missing:$outputFile", null)
                }
        } catch (routed: RoutedException) {
            if (routed.penalize == true) {
                flowFile = session.penalize(flowFile)
            }
            flowFile = session.putAttribute(flowFile, "error.message", routed.message + ": " + routed.cause.toString())
            session.transfer(flowFile, routed.relationship)
        }
    }
}
