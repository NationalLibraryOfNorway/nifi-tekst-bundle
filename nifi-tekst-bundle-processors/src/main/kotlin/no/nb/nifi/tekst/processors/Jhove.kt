package no.nb.nifi.tekst.processors

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import edu.harvard.hul.ois.jhove.App
import edu.harvard.hul.ois.jhove.JhoveBase
import no.nb.nifi.tekst.exceptions.RoutedException
import no.nb.nifi.tekst.util.AbstractRoutedProcessor
import org.apache.nifi.annotation.behavior.*
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.AllowableValue
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.Validator
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.ProcessorInitializationContext
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.util.StandardValidators
import java.io.FileInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import javax.xml.namespace.NamespaceContext
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathFactory
import no.nb.nifi.tekst.util.NiFiAttributes
import org.apache.nifi.components.ConfigVerificationResult
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.exception.ProcessException

@Tags("NB", "Validation", "JHOVE")
@CapabilityDescription(
    ("Validates a file with JHOVE. " +
            "Note that to force XML output complient with MIX10, and hence compatible with task-mets, " +
            "add <mixVersion>1.0</mixVersion> to jhove.conf")
)
@ReadsAttributes(ReadsAttribute(attribute = NiFiAttributes.FILENAME, description = ""))
@WritesAttributes(
    WritesAttribute(attribute = NiFiAttributes.FILENAME, description = ""),
    WritesAttribute(attribute = NiFiAttributes.COMPONENT_IDENTIFIER, description = ""),
    WritesAttribute(attribute = NiFiAttributes.COMPONENT_TYPE, description = ""),
    WritesAttribute(attribute = NiFiAttributes.COMPONENT_COUNT, description = "")
)
@SideEffectFree
@SupportsBatching
class Jhove : AbstractRoutedProcessor() {
    private var errorMessage: String? = null

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

        val MODULE: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("JHOVE module")
            .description(
                "Name of JHOVE module. Allowable values: AIFF-hul, ASCII-hul, JPEG-hul, gif-hul, " +
                        "HTML-hul, JPEG2000-hul, PDF-hul, TIFF-hul, UTF8-hul, XML-hul, PNG-gdm"
            )
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build()

        val CONFIG_PATH: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("Config path")
            .description("Path to jhove.conf")
            .required(true)
            .defaultValue("\${config.path}/jhove.conf")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build()

        val OUTPUT_PATH: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("Output folder path")
            .description("Path to folder for output file.")
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

        val COMPONENT_TYPE: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("Component type")
            .description("Component type attribute to assign the the JHOVE xml flowfile")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
            .description("Output file from JHOVE validation")
            .build()
    }

    override fun init(context: ProcessorInitializationContext?) {
        properties = ImmutableList.builder<PropertyDescriptor>()
            .add(INPUT_PATH)
            .add(OUTPUT_PATH)
            .add(MODULE)
            .add(CONFIG_PATH)
            .add(COMPONENT_TYPE)
            .add(BEHAVIOUR_ON_ERROR)
            .build()

        relationships = ImmutableSet.builder<Relationship>()
            .add(SUCCESS_RELATIONSHIP)
            .add(WELLFORMED_RELATIONSHIP)
            .add(FAIL_RELATIONSHIP)
            .add(JHOVE_OUTPUT_RELATIONSHIP)
            .build()
    }

    override fun verify(p0: ProcessContext?, p1: ComponentLog?, p2: MutableMap<String, String>?): MutableList<ConfigVerificationResult> {
        TODO("Not yet implemented")
    }

    @Throws(RoutedException::class)
    private fun runJhove(inputFile: Path, outputFile: Path, moduleName: String, configPath: String, errorMode: String): Int {
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

    override fun onTrigger(flowFile: FlowFile?, context: ProcessContext?, session: ProcessSession?) {
        if (flowFile == null) {
            throw ProcessException("FlowFile is null")
        }
        if (context == null) {
            throw ProcessException("ProcessContext is null")
        }
        if (session == null) {
            throw ProcessException("ProcessSession is null")
        }

        val outputPath = context.getProperty(OUTPUT_PATH).evaluateAttributeExpressions(flowFile).value
        val inputPath = context.getProperty(INPUT_PATH).evaluateAttributeExpressions(flowFile).value
        val module = context.getProperty(MODULE).evaluateAttributeExpressions(flowFile).value
        val configPath = context.getProperty(CONFIG_PATH).evaluateAttributeExpressions(flowFile).value
        val compType = context.getProperty(COMPONENT_TYPE).evaluateAttributeExpressions(flowFile).value

        try {
            val filename: String = flowFile.attributes[NiFiAttributes.FILENAME] ?: throw RoutedException(FAIL_RELATIONSHIP, message = "Filename attribute is missing in flowfile")
            val errorMode = context.getProperty(BEHAVIOUR_ON_ERROR).value

            if (module == null || module == "") {
                session.transfer(flowFile, SUCCESS_RELATIONSHIP)
                return
            }

            val inputFile = Paths.get(inputPath).resolve(filename)
            val outputFile = Paths.get(outputPath).resolve("JHOVE_$filename.xml")

            val res = runJhove(inputFile, outputFile, module, configPath, errorMode)

            if (Files.exists(outputFile)) {
                var outputFlowFile = session.create(flowFile)
                outputFlowFile = session.putAttribute(outputFlowFile, NiFiAttributes.FILENAME, outputFile.fileName.toString())
                if (compType != null) {
                    outputFlowFile = session.putAttribute(outputFlowFile, NiFiAttributes.COMPONENT_TYPE, compType)
                }

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
                            throw RoutedException(FAIL_RELATIONSHIP, false, "Jhove failed on file $inputFile: $errorMessage", null)
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
            flowFile!!.attributes["error.message"] = routed.message + ": " + routed.cause.toString()
            session.transfer(flowFile, routed.relationship)
        }
    }
}
