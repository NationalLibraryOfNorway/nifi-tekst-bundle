package no.nb.nifi.tekst.processors

import org.apache.nifi.util.TestRunners
import org.junit.Test
import kotlin.jvm.java

class GenerateDublinCoreTest {

    @Test
    fun `Test generate Dublin Core with valid type`() {
        // Test implementation goes here
        val runner = TestRunners.newTestRunner(GenerateDublinCore::class.java)
        runner.setProperty(GenerateDublinCore.TITLE, "Sample Title")
        runner.setProperty(GenerateDublinCore.TYPE, "Bok")

        runner.enqueue("Hello world")

        runner.run()

        // Get the output FlowFile from the success relationship
        val flowFiles = runner.getFlowFilesForRelationship("success")
        assert(flowFiles.size == 1) { "Expected one output FlowFile" }
        val xml = String(runner.getContentAsByteArray(flowFiles[0]))
        println("Generated XML:\n$xml")
    }
}
