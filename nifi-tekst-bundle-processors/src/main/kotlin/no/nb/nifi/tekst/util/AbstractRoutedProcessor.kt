package no.nb.nifi.tekst.util

import no.nb.nifi.tekst.exceptions.RoutedException
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.VerifiableProcessor
import org.apache.nifi.processor.exception.ProcessException

abstract class AbstractRoutedProcessor : AbstractProcessor(), VerifiableProcessor {
    protected var properties: MutableList<PropertyDescriptor> = mutableListOf()
    protected var relationships: MutableSet<Relationship> = mutableSetOf()


    @Throws(RoutedException::class, ProcessException::class)
    protected abstract fun onTrigger(flowFile: FlowFile?, context: ProcessContext?, session: ProcessSession?)

    @Throws(ProcessException::class)
    override fun onTrigger(context: ProcessContext?, session: ProcessSession) {
        var flowFile: FlowFile? = session.get() ?: return

        try {
            onTrigger(flowFile, context, session)
        } catch (e: RoutedException) {
            logger.log(e.getLogLevel(), e.getMessage(), e)
            NiFiAttributes.attachRoutedException(session, flowFile, e)

            if (e.isPenalize()) {
                flowFile = session.penalize(flowFile)
            }

            session.transfer(flowFile, e.getRelationship())
        }
    }
}