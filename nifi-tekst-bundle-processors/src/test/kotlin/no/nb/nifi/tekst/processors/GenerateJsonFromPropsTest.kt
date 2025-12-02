package no.nb.nifi.tekst.processors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.nifi.util.TestRunners
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import java.nio.charset.StandardCharsets

class GenerateJsonFromPropsTest {

    private val mapper = ObjectMapper()

    @Test
    fun `overwrite mode replaces existing content`() {
        val runner = TestRunners.newTestRunner(GenerateJsonFromProps::class.java)

        // output_mode = discard
        runner.setProperty(GenerateJsonFromProps.OUTPUT_MODE, "discard")

        // Dynamic properties -> nested JSON
        runner.setProperty("metadata.title.value", "My Book")
        runner.setProperty("metadata.title.lang", "eng")

        // Existing content that should be overwritten
        runner.enqueue("THIS SHOULD BE OVERWRITTEN")

        runner.run()

        runner.assertAllFlowFilesTransferred(GenerateJsonFromProps.REL_SUCCESS, 1)
        val out = runner.getFlowFilesForRelationship(GenerateJsonFromProps.REL_SUCCESS)[0]

        val content = String(out.toByteArray(), StandardCharsets.UTF_8)
        val json: JsonNode = mapper.readTree(content)

        // Expected: { "metadata": { "title": { "value": "My Book", "lang": "eng" } } }
        assertTrue(json.has("metadata"))
        val metadata = json.get("metadata")
        assertTrue(metadata.has("title"))
        val title = metadata.get("title")
        assertEquals("My Book", title.get("value").asText())
        assertEquals("eng", title.get("lang").asText())
    }

    @Test
    fun `merge mode merges with existing json`() {
        val runner = TestRunners.newTestRunner(GenerateJsonFromProps::class.java)

        // output_mode = merge
        runner.setProperty(GenerateJsonFromProps.OUTPUT_MODE, "merge")

        // Dynamic property to add or override inside metadata.title.value
        runner.setProperty("metadata.title.value", "New Title")

        // Existing JSON content
        val existingJson = """
            {
              "existing": "keep",
              "metadata": {
                "title": {
                  "lang": "eng"
                }
              }
            }
        """.trimIndent()

        runner.enqueue(existingJson)
        runner.run()

        runner.assertAllFlowFilesTransferred(GenerateJsonFromProps.REL_SUCCESS, 1)
        val out = runner.getFlowFilesForRelationship(GenerateJsonFromProps.REL_SUCCESS)[0]

        val content = String(out.toByteArray(), StandardCharsets.UTF_8)
        val json: JsonNode = mapper.readTree(content)

        // "existing" should still be there
        assertEquals("keep", json.get("existing").asText())

        // metadata.title.lang should be preserved, value should be added/overwritten
        val metadata = json.get("metadata")
        val title = metadata.get("title")
        assertEquals("eng", title.get("lang").asText())
        assertEquals("New Title", title.get("value").asText())
    }

    @Test
    fun `nested attributes with expression language`() {
        val runner = TestRunners.newTestRunner(GenerateJsonFromProps::class.java)

        // Default is "discard", that is fine here, but set explicitly
        runner.setProperty(GenerateJsonFromProps.OUTPUT_MODE, "discard")

        // Dynamic properties using EL to pull from FlowFile attributes
        runner.setProperty("metadata.type.value", "\${typeAttr}")
        runner.setProperty("metadata.type.lang", "nor")
        runner.setProperty("metadata.identifier.value", "\${idAttr}")
        runner.setProperty("metadata.title.value", "\${titleAttr}")
        runner.setProperty("metadata.title.lang", "eng")

        // FlowFile attributes
        val attributes = mapOf(
            "typeAttr" to "Bilde",
            "idAttr" to "URN:NBN:no-nb_plfut_00001",
            "titleAttr" to "My Book Title"
        )

        // Body is irrelevant here; we only test nested structure from attributes
        runner.enqueue("{}", attributes)
        runner.run()

        runner.assertAllFlowFilesTransferred(GenerateJsonFromProps.REL_SUCCESS, 1)
        val out = runner.getFlowFilesForRelationship(GenerateJsonFromProps.REL_SUCCESS)[0]

        val content = String(out.toByteArray(), StandardCharsets.UTF_8)
        val json: JsonNode = mapper.readTree(content)

        /*
           Expected structure (order not important):
           {
             "metadata": {
               "type": {
                 "value": "Bilde",
                 "lang": "nor"
               },
               "identifier": {
                 "value": "URN:NBN:no-nb_plfut_00001"
               },
               "title": {
                 "value": "My Book Title",
                 "lang": "eng"
               }
             }
           }
         */

        assertTrue(json.has("metadata"))
        val metadata = json.get("metadata")

        // type
        val type = metadata.get("type")
        assertEquals("Bilde", type.get("value").asText())
        assertEquals("nor", type.get("lang").asText())

        // identifier
        val identifier = metadata.get("identifier")
        assertEquals("URN:NBN:no-nb_plfut_00001", identifier.get("value").asText())

        // title
        val title = metadata.get("title")
        assertEquals("My Book Title", title.get("value").asText())
        assertEquals("eng", title.get("lang").asText())
    }

}
