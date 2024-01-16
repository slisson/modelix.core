package org.modelix.metamodel.generator

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.withIndent
import org.modelix.metamodel.generator.internal.ConceptFileGenerator
import org.modelix.metamodel.generator.internal.EnumFileGenerator
import org.modelix.metamodel.generator.internal.LanguageFileGenerator
import org.modelix.metamodel.generator.internal.MetaPropertiesInterfaceGenerator
import org.modelix.metamodel.generator.internal.ModelQLExtensionsGenerator
import org.modelix.metamodel.generator.internal.RegistrationHelperGenerator
import java.nio.file.Path

class MetaModelGenerator(
    private val outputDir: Path,
    private val nameConfig: NameConfig = NameConfig(),
    private val modelQlOutputDir: Path? = null,
    private val conceptPropertiesInterfaceName: String? = null,
    private val alwaysUseNonNullableProperties: Boolean = true,
) {

    companion object {
        const val HEADER_COMMENT = "\ngenerated by modelix model-api-gen \n"
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
        RegistrationHelperGenerator(
            classFqName,
            languages as ProcessedLanguageSet,
            outputDir,
            nameConfig,
        ).generateFile()
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
            LanguageFileGenerator(language, outputDir, nameConfig).generateFile()

            for (enum in language.getEnums()) {
                EnumFileGenerator(enum, outputDir).generateFile()
            }

            for (concept in language.getConcepts()) {
                ConceptFileGenerator(
                    concept,
                    outputDir,
                    nameConfig,
                    conceptPropertiesInterfaceName,
                    alwaysUseNonNullableProperties,
                ).generateFile()
                if (modelQlOutputDir != null && concept.getOwnRoles().isNotEmpty()) {
                    ModelQLExtensionsGenerator(
                        concept,
                        modelQlOutputDir,
                        nameConfig,
                        alwaysUseNonNullableProperties,
                    ).generateFile()
                }
            }
        }
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
