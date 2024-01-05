/*
 * Copyright (c) 2024.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.modelix.metamodel.generator

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.TypeVariableName
import com.squareup.kotlinpoet.asClassName
import com.squareup.kotlinpoet.asTypeName
import org.modelix.metamodel.GeneratedProperty
import org.modelix.metamodel.IConceptOfTypedNode
import org.modelix.metamodel.INonAbstractConcept
import org.modelix.metamodel.ITypedConcept
import org.modelix.model.api.IConcept
import kotlin.reflect.KClass

internal class ConceptWrapperInterfaceGenerator(private val concept: ProcessedConcept, private val generator: MetaModelGenerator) {
    fun generate(): TypeSpec {
        val nodeT = TypeVariableName("NodeT", concept.nodeWrapperInterfaceType(), variance = KModifier.OUT)

        return TypeSpec.interfaceBuilder(concept.conceptWrapperInterfaceClass()).runBuild {
            addDeprecationIfNecessary(concept)
            addTypeVariable(nodeT)
            addSuperinterfaces(nodeT)
            for (feature in concept.getOwnRoles()) {
                when (feature) {
                    is ProcessedProperty -> addConceptWrapperInterfaceProperty(feature)
                    is ProcessedChildLink -> addConceptWrapperInterfaceChildLink(feature)
                    is ProcessedReferenceLink -> addConceptWrapperInterfaceReferenceLink(feature)
                }
            }
            addCompanionObject()
        }
    }

    private fun TypeSpec.Builder.addCompanionObject() {
        val getInstanceInterfaceFun = FunSpec.builder(IConceptOfTypedNode<*>::getInstanceInterface.name).runBuild {
            addModifiers(KModifier.OVERRIDE)
            returns(KClass::class.asTypeName().parameterizedBy(concept.nodeWrapperInterfaceType()))
            addStatement("return %T::class", concept.nodeWrapperInterfaceType())
        }

        val untypedFun = FunSpec.builder(ITypedConcept::untyped.name).runBuild {
            returns(IConcept::class)
            addModifiers(KModifier.OVERRIDE)
            addStatement("return %T", concept.conceptObjectType())
        }

        val companionObj = TypeSpec.companionObjectBuilder().runBuild {
            addSuperinterface(concept.conceptWrapperInterfaceType())
            val t = if (concept.abstract) IConceptOfTypedNode::class else INonAbstractConcept::class
            addSuperinterface(t.asTypeName().parameterizedBy(concept.nodeWrapperInterfaceType()))
            addFunction(getInstanceInterfaceFun)
            addFunction(untypedFun)
            addConceptMetaPropertiesIfNecessary()
        }
        addType(companionObj)
    }

    private fun TypeSpec.Builder.addConceptMetaPropertiesIfNecessary() {
        if (generator.conceptPropertiesInterfaceName == null) return

        concept.metaProperties.forEach { (key, value) ->
            val propertySpec = PropertySpec.builder(key, String::class.asTypeName()).runBuild {
                addModifiers(KModifier.OVERRIDE)
                initializer("%S", value)
            }

            addProperty(propertySpec)
        }
    }

    private fun TypeSpec.Builder.addConceptWrapperInterfaceReferenceLink(feature: ProcessedReferenceLink) {
        val propertySpec = PropertySpec.builder(feature.generatedName, feature.generatedReferenceLinkType()).runBuild {
            getter(FunSpec.getterBuilder().addCode(feature.returnKotlinRef()).build())
            addDeprecationIfNecessary(feature)
        }

        addProperty(propertySpec)
    }

    private fun TypeSpec.Builder.addConceptWrapperInterfaceChildLink(feature: ProcessedChildLink) {
        val propertySpec = PropertySpec.builder(feature.generatedName, feature.generatedChildLinkType()).runBuild {
            getter(FunSpec.getterBuilder().addCode(feature.returnKotlinRef()).build())
            addDeprecationIfNecessary(feature)
        }

        addProperty(propertySpec)
    }

    private fun TypeSpec.Builder.addConceptWrapperInterfaceProperty(feature: ProcessedProperty) {
        val propertySpec = PropertySpec.builder(
            name = feature.generatedName,
            type = GeneratedProperty::class.asClassName().parameterizedBy(feature.asKotlinType()),
        ).runBuild {
            val getterSpec = FunSpec.getterBuilder().runBuild {
                addCode(feature.returnKotlinRef())
            }
            getter(getterSpec)
            addDeprecationIfNecessary(feature)
        }

        addProperty(propertySpec)
    }

    private fun TypeSpec.Builder.addSuperinterfaces(nodeT: TypeVariableName) {
        addSuperinterface(IConceptOfTypedNode::class.asTypeName().parameterizedBy(nodeT))
        for (extended in concept.getDirectSuperConcepts()) {
            addSuperinterface(extended.conceptWrapperInterfaceClass().parameterizedBy(nodeT))
        }

        val conceptPropertiesInterfaceName = generator.conceptPropertiesInterfaceName

        if (conceptPropertiesInterfaceName != null && concept.extends.isEmpty()) {
            val pckgName = conceptPropertiesInterfaceName.substringBeforeLast(".")
            val interfaceName = conceptPropertiesInterfaceName.substringAfterLast(".")
            addSuperinterface(ClassName(pckgName, interfaceName))
        }
    }

    private fun ProcessedProperty.asKotlinType() = generator.run { asKotlinType() }
    private fun ProcessedConcept.conceptObjectType() = generator.run { conceptObjectType() }
    private fun ProcessedConcept.conceptWrapperInterfaceType() = generator.run { conceptWrapperInterfaceType() }
    private fun ProcessedConcept.nodeWrapperInterfaceType() = generator.run { nodeWrapperInterfaceType() }
    private fun ProcessedConcept.conceptWrapperInterfaceClass() = generator.run { conceptWrapperInterfaceClass() }

    private fun ProcessedRole.returnKotlinRef() = CodeBlock.of("return %T.%N", concept.conceptObjectType(), generatedName)
    private fun ProcessedChildLink.generatedChildLinkType() = generator.run { generatedChildLinkType() }
    private fun ProcessedReferenceLink.generatedReferenceLinkType() = generator.run { generatedReferenceLinkType() }
}
