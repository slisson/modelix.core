package org.modelix.metamodel.generator

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.asClassName
import com.squareup.kotlinpoet.asTypeName
import com.squareup.kotlinpoet.withIndent
import org.modelix.metamodel.GeneratedChildListLink
import org.modelix.metamodel.GeneratedMandatorySingleChildLink
import org.modelix.metamodel.GeneratedReferenceLink
import org.modelix.metamodel.GeneratedSingleChildLink
import org.modelix.metamodel.ITypedConcept
import org.modelix.model.data.EnumPropertyType
import org.modelix.model.data.Primitive
import org.modelix.model.data.PrimitivePropertyType
import org.modelix.modelql.core.IFluxStep
import org.modelix.modelql.core.IMonoStep
import org.modelix.modelql.core.IProducingStep
import org.modelix.modelql.typed.TypedModelQL
import java.nio.file.Path

class MetaModelGenerator(
    val outputDir: Path,
    val nameConfig: NameConfig = NameConfig(),
    val modelqlOutputDir: Path? = null,
    val conceptPropertiesInterfaceName: String? = null,
) {
    var alwaysUseNonNullableProperties: Boolean = true

    companion object {
        const val HEADER_COMMENT = "\ngenerated by modelix model-api-gen \n"
    }

    internal fun ProcessedProperty.asKotlinType(): TypeName {
        val nonNullableType = when (type) {
            is PrimitivePropertyType -> when ((type as PrimitivePropertyType).primitive) {
                Primitive.STRING -> String::class.asTypeName()
                Primitive.BOOLEAN -> Boolean::class.asTypeName()
                Primitive.INT -> Int::class.asTypeName()
            }
            is EnumPropertyType -> {
                val enumType = (type as EnumPropertyType)
                ClassName(enumType.pckg, enumType.enumName)
            }
            else -> { throw RuntimeException("Unexpected property type: $type") }
        }
        return if (!optional || alwaysUseNonNullableProperties) nonNullableType else nonNullableType.copy(nullable = true)
    }

    private fun FileSpec.write() {
        writeTo(outputDir)
    }

    private fun ProcessedLanguage.packageDir(): Path {
        val packageName = name
        var packageDir = outputDir
        if (packageName.isNotEmpty()) {
            for (packageComponent in packageName.split('.').dropLastWhile { it.isEmpty() }) {
                packageDir = packageDir.resolve(packageComponent)
            }
        }
        return packageDir
    }

    fun generateRegistrationHelper(classFqName: String, languages: IProcessedLanguageSet) {
        RegistrationHelperGenerator(classFqName, languages as ProcessedLanguageSet, this).generateFile()
    }

    fun generate(languages: IProcessedLanguageSet) {
        generate(languages as ProcessedLanguageSet)
    }

    private fun generate(languages: ProcessedLanguageSet) {
        if (conceptPropertiesInterfaceName != null) {
            MetaPropertiesInterfaceGenerator(languages, outputDir, conceptPropertiesInterfaceName).generateFile()
        }

        for (language in languages.getLanguages()) {
            language.packageDir().toFile().listFiles()?.filter { it.isFile }?.forEach { it.delete() }
            LanguageFileGenerator(language, this).generateFile()

            for (enum in language.getEnums()) {
                EnumFileGenerator(enum, outputDir).generateFile()
            }

            for (concept in language.getConcepts()) {
                ConceptFileGenerator(concept, this).generateFile()
                if (modelqlOutputDir != null && concept.getOwnRoles().isNotEmpty()) {
                    generateModelQLFile(concept)
                }
            }
        }
    }

    private fun generateModelQLFile(concept: ProcessedConcept) {
        FileSpec.builder("org.modelix.modelql.gen." + concept.language.name, concept.name)
            .addFileComment(HEADER_COMMENT)
            .apply {
                for (feature in concept.getOwnRoles()) {
                    val receiverType = Iterable::class.asTypeName().parameterizedBy(concept.nodeWrapperInterfaceType())
                    when (feature) {
                        is ProcessedProperty -> {
                            for (stepType in listOf(IMonoStep::class.asTypeName(), IFluxStep::class.asTypeName())) {
                                val inputType = stepType.parameterizedBy(concept.nodeWrapperInterfaceType())
                                val outputElementType = when (feature.type) {
                                    is EnumPropertyType -> String::class.asTypeName().copy(nullable = true)
                                    is PrimitivePropertyType -> feature.asKotlinType()
                                }
                                val outputType = stepType.parameterizedBy(outputElementType)
                                val functionName = when (val type = feature.type) {
                                    is EnumPropertyType -> "rawProperty"
                                    is PrimitivePropertyType -> when (type.primitive) {
                                        Primitive.STRING -> "stringProperty"
                                        Primitive.BOOLEAN -> "booleanProperty"
                                        Primitive.INT -> "intProperty"
                                    }
                                }
                                addProperty(
                                    PropertySpec.builder(feature.generatedName, outputType)
                                        .receiver(inputType)
                                        .getter(
                                            FunSpec.getterBuilder()
                                                .addStatement(
                                                    "return %T.%N(this, %T.%N)",
                                                    TypedModelQL::class.asTypeName(),
                                                    functionName,
                                                    concept.conceptWrapperInterfaceClass(),
                                                    feature.generatedName,
                                                )
                                                .build(),
                                        )
                                        .build(),
                                )
                            }

                            val inputStepType = IMonoStep::class.asTypeName()
                                .parameterizedBy(concept.nodeWrapperInterfaceType())
                            addFunction(
                                FunSpec.builder(feature.setterName())
                                    .returns(inputStepType)
                                    .receiver(inputStepType)
                                    .addParameter("value", IMonoStep::class.asTypeName().parameterizedBy(feature.asKotlinType()))
                                    .addStatement(
                                        "return %T.setProperty(this, %T.%N, value)",
                                        TypedModelQL::class.asTypeName(),
                                        concept.conceptWrapperInterfaceClass(),
                                        feature.generatedName,
                                    )
                                    .build(),
                            )
                        }

                        is ProcessedChildLink -> {
                            val targetType = feature.type.resolved.nodeWrapperInterfaceType()

                            val inputStepType = (if (feature.multiple) IProducingStep::class else IMonoStep::class).asTypeName()
                            val outputStepType = (if (feature.multiple) IFluxStep::class else IMonoStep::class).asTypeName()
                            val inputType = inputStepType.parameterizedBy(concept.nodeWrapperInterfaceType())
                            val isOptionalSingle = feature.optional && !feature.multiple
                            val outputType = outputStepType.parameterizedBy(
                                targetType.copy(nullable = isOptionalSingle),
                            )
                            addProperty(
                                PropertySpec.builder(feature.generatedName, outputType)
                                    .receiver(inputType)
                                    .getter(
                                        FunSpec.getterBuilder()
                                            .addStatement(
                                                "return %T.children(this, %T.%N)",
                                                TypedModelQL::class.asTypeName(),
                                                concept.conceptWrapperInterfaceClass(),
                                                feature.generatedName,
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            val returnType = IMonoStep::class.asTypeName().parameterizedBy(targetType)
                            val receiverType = IMonoStep::class.asTypeName().parameterizedBy(concept.nodeWrapperInterfaceType())
                            val conceptParameter = ParameterSpec.builder("concept", ITypedConcept::class.asTypeName()).apply {
                                if (!feature.type.resolved.abstract) {
                                    defaultValue("%T", feature.type.resolved.conceptWrapperInterfaceClass())
                                }
                            }.build()

                            if (feature.multiple) {
                                addFunction(
                                    FunSpec.builder(feature.adderMethodName())
                                        .returns(returnType)
                                        .receiver(receiverType)
                                        .addParameter(conceptParameter)
                                        .addParameter(
                                            ParameterSpec.builder("index", Int::class.asTypeName())
                                                .defaultValue("-1")
                                                .build(),
                                        )
                                        .addStatement(
                                            "return %T.addNewChild(this, %T.%N, index, concept)",
                                            TypedModelQL::class.asTypeName(),
                                            concept.conceptObjectType(),
                                            feature.generatedName,
                                        )
                                        .build(),
                                )
                            } else {
                                addFunction(
                                    FunSpec.builder(feature.setterName())
                                        .returns(returnType)
                                        .receiver(receiverType)
                                        .addParameter(conceptParameter)
                                        .addStatement(
                                            "return %T.setChild(this, %T.%N, concept)",
                                            TypedModelQL::class.asTypeName(),
                                            concept.conceptObjectType(),
                                            feature.generatedName,
                                        )
                                        .build(),
                                )
                            }
                        }

                        is ProcessedReferenceLink -> {
                            val targetType =
                                feature.type.resolved.nodeWrapperInterfaceType().copy(nullable = feature.optional)

                            for (stepType in listOf(IMonoStep::class.asTypeName(), IFluxStep::class.asTypeName())) {
                                val inputType = stepType.parameterizedBy(concept.nodeWrapperInterfaceType())
                                val outputType = stepType.parameterizedBy(targetType.copy(nullable = false))
                                val outputTypeNullable = stepType.parameterizedBy(targetType.copy(nullable = true))
                                addProperty(
                                    PropertySpec.builder(feature.generatedName, outputType)
                                        .receiver(inputType)
                                        .getter(
                                            FunSpec.getterBuilder()
                                                .addStatement(
                                                    "return %T.reference(this, %T.%N)",
                                                    TypedModelQL::class.asTypeName(),
                                                    concept.conceptWrapperInterfaceClass(),
                                                    feature.generatedName,
                                                )
                                                .build(),
                                        )
                                        .build(),
                                )
                                addProperty(
                                    PropertySpec.builder(feature.generatedName + "_orNull", outputTypeNullable)
                                        .receiver(inputType)
                                        .getter(
                                            FunSpec.getterBuilder()
                                                .addStatement(
                                                    "return %T.referenceOrNull(this, %T.%N)",
                                                    TypedModelQL::class.asTypeName(),
                                                    concept.conceptWrapperInterfaceClass(),
                                                    feature.generatedName,
                                                )
                                                .build(),
                                        )
                                        .build(),
                                )
                            }

                            val inputStepType = IMonoStep::class.asTypeName()
                                .parameterizedBy(concept.nodeWrapperInterfaceType())
                            addFunction(
                                FunSpec.builder(feature.setterName())
                                    .returns(inputStepType)
                                    .receiver(inputStepType)
                                    .addParameter(
                                        "target",
                                        IMonoStep::class.asTypeName().parameterizedBy(targetType)
                                            .let { if (feature.optional) it.copy(nullable = true) else it },
                                    )
                                    .addStatement(
                                        "return %T.setReference(this, %T.%N, target)",
                                        TypedModelQL::class.asTypeName(),
                                        concept.conceptWrapperInterfaceClass(),
                                        feature.generatedName,
                                    )
                                    .build(),
                            )
                        }
                    }
                }
            }
            .build().writeTo(modelqlOutputDir!!)
    }

    internal fun ProcessedConcept.conceptWrapperInterfaceType() =
        conceptWrapperInterfaceClass().parameterizedBy(nodeWrapperInterfaceType())

    internal fun ProcessedConcept.conceptWrapperInterfaceClass() =
        ClassName(language.name, nameConfig.typedConcept(name))

    internal fun ProcessedLanguage.generatedClassName() = ClassName(name, nameConfig.languageClass(name))
    private fun ProcessedConcept.nodeWrapperInterfaceName() = nameConfig.typedNode(name)
    private fun ProcessedConcept.nodeWrapperImplName() = nameConfig.typedNodeImpl(name)
    internal fun ProcessedConcept.conceptObjectName() = nameConfig.untypedConcept(name)
    internal fun ProcessedConcept.conceptTypeAliasName() = nameConfig.conceptTypeAlias(name)
    // private fun ProcessedConcept.conceptWrapperImplName() = nameConfig.conceptWrapperImplName(name)
    // private fun ProcessedConcept.conceptWrapperInterfaceName() = nameConfig.conceptWrapperInterfaceName(name)

    // private fun ProcessedConcept.getConceptFqName() = language.name + "." + name
    internal fun ProcessedConcept.conceptObjectType() = ClassName(language.name, conceptObjectName())
    internal fun ProcessedConcept.nodeWrapperImplType() = ClassName(language.name, nodeWrapperImplName())
    internal fun ProcessedConcept.nodeWrapperInterfaceType() = ClassName(language.name, nodeWrapperInterfaceName())

    // private fun ProcessedRole.kotlinRef() = CodeBlock.of("%T.%N", concept.conceptObjectType(), generatedName)

    internal fun ProcessedChildLink.generatedChildLinkType(): TypeName {
        val childConcept = type.resolved
        val linkClass = if (multiple) {
            GeneratedChildListLink::class
        } else {
            if (optional) GeneratedSingleChildLink::class else GeneratedMandatorySingleChildLink::class
        }
        return linkClass.asClassName().parameterizedBy(
            childConcept.nodeWrapperInterfaceType(),
            childConcept.conceptWrapperInterfaceType(),
        )
    }

    internal fun ProcessedReferenceLink.generatedReferenceLinkType(): TypeName {
        val targetConcept = type.resolved
        return GeneratedReferenceLink::class.asClassName().parameterizedBy(
            targetConcept.nodeWrapperInterfaceType(),
            targetConcept.conceptWrapperInterfaceType(),
        )
    }
}

internal fun List<TypeName>.toListLiteralCodeBlock(): CodeBlock {
    val list = this
    return CodeBlock.builder().apply {
        add("return listOf(\n")
        withIndent {
            for (element in list) {
                add("%T,\n", element)
            }
        }
        add(")")
    }.build()
}

internal fun generateDeprecationAnnotation(message: String): AnnotationSpec {
    val annotationBuilder = AnnotationSpec.builder(Deprecated::class)
    if (message.isNotEmpty()) { annotationBuilder.addMember("message = %S", message) }
    return annotationBuilder.build()
}

internal fun TypeSpec.Builder.addDeprecationIfNecessary(deprecatable: IProcessedDeprecatable): TypeSpec.Builder {
    return deprecatable.deprecationMessage?.let { addAnnotation(generateDeprecationAnnotation(it)) } ?: this
}

internal fun PropertySpec.Builder.addDeprecationIfNecessary(deprecatable: IProcessedDeprecatable): PropertySpec.Builder {
    return deprecatable.deprecationMessage?.let { addAnnotation(generateDeprecationAnnotation(it)) } ?: this
}

internal inline fun FunSpec.Builder.runBuild(crossinline body: FunSpec.Builder.() -> Unit): FunSpec {
    body()
    return build()
}

internal inline fun TypeSpec.Builder.runBuild(crossinline body: TypeSpec.Builder.() -> Unit): TypeSpec {
    body()
    return build()
}

internal inline fun FileSpec.Builder.runBuild(crossinline body: FileSpec.Builder.() -> Unit): FileSpec {
    body()
    return build()
}

internal inline fun PropertySpec.Builder.runBuild(crossinline body: PropertySpec.Builder.() -> Unit): PropertySpec {
    body()
    return build()
}

internal inline fun CodeBlock.Builder.runBuild(crossinline body: CodeBlock.Builder.() -> Unit): CodeBlock {
    body()
    return build()
}
