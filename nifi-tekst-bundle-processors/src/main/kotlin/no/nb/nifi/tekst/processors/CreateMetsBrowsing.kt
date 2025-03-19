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


import no.nb.nifi.tekst.mets.MetsBrowsingGenerator
import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.*
import org.apache.nifi.processor.util.StandardValidators
import java.util.*

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
            .displayName("Object folder")
            .description("Object folder on server to download the files to")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build()

        // TODO consider using a "OCR" folder and "Access image" folder instead of the object folder.

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

        // Extract properties
        val objectFolder = context.getProperty("object_folder").evaluateAttributeExpressions(flowFile).value
        val metsBrowsingGenerator = MetsBrowsingGenerator()

        try {
            metsBrowsingGenerator.generateMetsBrowsing(objectFolder)
        } catch (e: Exception) {
            logger.error("Failed to METS", e)
            session.transfer(flowFile, REL_FAILURE)
            return
        }

        session.transfer(flowFile, REL_SUCCESS)
    }

}