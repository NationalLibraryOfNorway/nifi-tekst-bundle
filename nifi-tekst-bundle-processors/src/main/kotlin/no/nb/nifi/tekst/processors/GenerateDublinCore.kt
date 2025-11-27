package no.nb.nifi.tekst.processors

import no.nb.nifi.tekst.utils.tag
import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.ProcessorInitializationContext
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.util.StandardValidators
import java.util.Collections
import java.util.HashSet

@Tags("NB", "Tekst", "Text", "Dublincore", "Generate", "Metadata")
@CapabilityDescription
    ("This processor generates dublincore metadata in an XML. Language attributes uses ISO 639-3 standard.")

@SideEffectFree
class GenerateDublinCore : AbstractProcessor() {
    private var descriptors: MutableList<PropertyDescriptor> = mutableListOf()

    private var relationships: MutableSet<Relationship> = mutableSetOf()

    companion object {
        val TYPE: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("type")
            .displayName("Medietype *")
            .description(
                "Type ressurs/medietype. Tillatte typer for Tekst er: " +
                        "Bok, Avis, Tidsskrift, Artikkel, Småtrykk, Brev, Epost, Manuskript, " +
                        "Musikkmanuskript, Noter, Programrapport, Programstatistikk"
            )
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .allowableValues(
                "Bok",
                "Avis",
                "Tidsskrift",
                "Artikkel",
                "Småtrykk",
                "Brev",
                "Epost",
                "Manuskript",
                "Musikkmanuskript",
                "Noter",
                "Programrapport",
                "Programstatistikk"
            )
            .required(true)
            .defaultValue("Avis")
            .build()

        //TODO: Identifier

        val TITLE: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("title")
            .displayName("Tittel")
            .description(
                "Navn gitt til ressursen. Der tittel mangler er anbefalt praksis " +
                        "å gi ressursen en “meningsbærende” tittel."
            )
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        val TITLE_LANG: PropertyDescriptor = PropertyDescriptor.Builder()
            // TODO: Utvid med allowable values for gyldige ISO 639-3 koder for språk vi benytter?
            .name("title_language")
            .displayName("Tittel språk *")
            .description(
                "Språk for tittel, angitt med ISO 639-3 kode. " +
                        "Hvis ikke angitt, settes språkkode til 'nor' (norsk)."
            )
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .defaultValue("nor")
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
        descriptors.add(TYPE)
        descriptors.add(TITLE)
        descriptors.add(TITLE_LANG)

        descriptors = Collections.unmodifiableList(descriptors)

        relationships = HashSet()
        relationships.add(REL_SUCCESS)
        relationships.add(REL_FAILURE)

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
        val type = context.getProperty("type").evaluateAttributeExpressions(flowFile).value
        if(type == null){
            logger.error("Type is required")
            session.transfer(flowFile, REL_FAILURE)
            return
        }
        val title = context.getProperty("title").evaluateAttributeExpressions(flowFile).value
        if(title == null){
            logger.error("Title is required")
            session.transfer(flowFile, REL_FAILURE)
            return
        }
        val titleLang = context.getProperty("title_language").evaluateAttributeExpressions(flowFile).value ?: "nor"

        val dublinCoreXml = tag("metadata") {
            // dcmitype: https://www.dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd
            // qualifieddc: https://www.dublincore.org/schemas/xmls/qdc/2008/02/11/qualifieddc.xsd
            attr("xmlns:dc", "https://www.dublincore.org/schemas/xmls/qdc/2008/02/11/dc.xsd")
            attr("xmlns:dcterms", "https://www.dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd")
            tag("dc:title", title) {
                attr("xml:lang", titleLang)
            }
            tag("dc:type", type) {
                attr("xml:lang", "nor")
            }
        }.render(0)

        // Write the generated XML to the FlowFile content
        val updatedFlowFile = session.write(flowFile) { outputStream ->
            outputStream.write(dublinCoreXml.toByteArray(Charsets.UTF_8))
        }
        session.transfer(updatedFlowFile, REL_SUCCESS)
    }
}
