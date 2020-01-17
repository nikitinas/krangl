package krangl.typed

import krangl.*
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.*

@Target(AnnotationTarget.CLASS)
annotation class DataFrameType(val isOpen: Boolean = true)

@Target(AnnotationTarget.PROPERTY)
annotation class ColumnType(val type: KClass<out DataCol>)

@Target(AnnotationTarget.PROPERTY)
annotation class ColumnName(val name: String)

enum class CodeGenerationMode {
    FullNames,
    ShortNames
}

object CodeGenerator {

    var mode = CodeGenerationMode.ShortNames

    internal fun getColumnType(valueType: KType) =
            when (valueType.classifier) {
                Int::class -> IntCol::class
                Double::class -> DoubleCol::class
                Boolean::class -> BooleanCol::class
                String::class -> StringCol::class
                else -> AnyCol::class
            }.createType()

    data class FieldInfo(val columnName: String, val fieldName: String, val fieldType: KType, val columnType: KType) {
        fun isSubFieldOf(other: FieldInfo) =
                columnName == other.columnName && fieldType.isSubtypeOf(other.fieldType) && columnType.isSubtypeOf(other.columnType)
    }

    internal class Scheme(val values: List<FieldInfo>) {

        val byColumn: Map<String, FieldInfo> = values.map { it.columnName to it }.toMap()
        val byField: Map<String, FieldInfo> = values.map { it.fieldName to it }.toMap()

        fun contains(field: FieldInfo) = byField[field.fieldName]?.equals(field) ?: false

        fun isSuperTo(other: Scheme) =
                values.all {
                    other.byColumn[it.columnName]?.isSubFieldOf(it) ?: false
                }

        override fun equals(other: Any?): Boolean {
            val scheme = other as? Scheme ?: return false
            if (scheme.values.size != values.size) return false
            return values.all {
                val otherEntry = other.byColumn[it.columnName] ?: return false
                otherEntry.equals(it)
            }
        }

        override fun hashCode(): Int {
            return values.sortedBy { it.fieldName }.hashCode()
        }
    }

    internal data class GeneratedMarker(val scheme: Scheme, val kclass: KClass<*>, val isOpen: Boolean)

    private val generatedMarkers = mutableListOf<GeneratedMarker>()

    private fun render(clazz: KClass<*>) = when (mode) {
        CodeGenerationMode.ShortNames -> clazz.simpleName
        CodeGenerationMode.FullNames -> clazz.qualifiedName
    }

    private fun shortTypeName(type: KType) =
            if (type.arguments.size > 0) null
            else (type.classifier as? KClass<*>)?.simpleName + if (type.isMarkedNullable) "?" else ""

    private fun render(type: KType) = when (mode) {
        CodeGenerationMode.FullNames -> type.toString()
        CodeGenerationMode.ShortNames -> shortTypeName(type) ?: type.toString()
    }

    private fun generateTypedCode(scheme: Scheme, markerType: String): List<String> {
        val declarations = mutableListOf<String>()
        val dfTypename = render(TypedDataFrame::class) + "<$markerType>"
        val rowTypename = render(TypedDataFrameRow::class) + "<$markerType>"
        scheme.values.sortedBy { it.columnName }.forEach { field ->
            val getter = "this[\"${field.columnName}\"]"
            val name = field.fieldName
            val valueType = render(field.fieldType)
            val columnType = render(field.columnType)
            declarations.add(codeForProperty(dfTypename, name, columnType, getter))
            declarations.add(codeForProperty(rowTypename, name, valueType, getter))
        }
        return declarations
    }

    private fun getSchemeFields(clazz: KClass<*>, withBaseTypes: Boolean): Map<String, FieldInfo> {
        val result = mutableMapOf<String, FieldInfo>()
        if (withBaseTypes)
            clazz.superclasses.forEach { result.putAll(getSchemeFields(it, withBaseTypes)) }
        result.putAll(clazz.declaredMemberProperties.map {
            val columnName = it.findAnnotation<ColumnName>()?.name ?: it.name
            val columnType = it.findAnnotation<ColumnType>()?.type?.createType() ?: getColumnType(it.returnType)
            it.name to FieldInfo(columnName, it.name, it.returnType, columnType)
        })
        return result
    }

    private fun getScheme(kclass: KClass<*>, withBaseTypes: Boolean) = Scheme(getSchemeFields(kclass, withBaseTypes).values.toList())

    private fun generateValidFieldName(columnName: String) = columnName.replace(" ", "_")

    internal fun getScheme(columns: Iterable<DataCol>) = Scheme(columns.map { FieldInfo(it.name, generateValidFieldName(it.name), it.valueType, it.javaClass.kotlin.createType()) })

    private val DataFrame.scheme: Scheme
        get() = getScheme(cols)

    private fun codeForProperty(typeName: String, name: String, propertyType: String, getter: String): String {
        return "val $typeName.$name: $propertyType get() = ($getter) as $propertyType"
    }

    fun generate(interfaze: KClass<*>): List<String> {
        val annotation = interfaze.findAnnotation<DataFrameType>() ?: return emptyList()
        val ownSet = getScheme(interfaze, withBaseTypes = false)
        val fullSet = getScheme(interfaze, withBaseTypes = true)
        val typeName = interfaze.qualifiedName!!
        val result = generateTypedCode(ownSet, typeName)
        generatedMarkers.add(GeneratedMarker(fullSet, interfaze, annotation.isOpen))
        return result
    }

    private fun Scheme.getAllBaseMarkers() = generatedMarkers
            .filter { it.scheme.isSuperTo(this) }

    private fun List<GeneratedMarker>.onlyLeafs(): List<GeneratedMarker> {
        val skip = flatMap { it.kclass.allSuperclasses }.toSet()
        return filter { !skip.contains(it.kclass) }
    }

    private fun Scheme.getBaseMarkers() = getAllBaseMarkers().onlyLeafs()

    private fun Scheme.getRequiredBaseMarkers() = generatedMarkers
            .filter { it.isOpen && it.scheme.isSuperTo(this) }

    private val processedProperties = mutableSetOf<KProperty<*>>()

    fun generate(df: DataFrame, property: KProperty<*>): List<String> {

        fun KClass<*>.implements(targetBaseMarkers: Iterable<KClass<*>>): Boolean {
            val superclasses = allSuperclasses + this
            return targetBaseMarkers.all { superclasses.contains(it) }
        }

        val markerType: String?
        var targetScheme = df.scheme
        val wasProcessedBefore = property in processedProperties
        processedProperties.add(property)
        val isMutable = property is KMutableProperty

        // maybe property is already properly typed, let's do some checks
        val currentMarkerType = getMarkerType(property.returnType)
        if (currentMarkerType != null) {
            // if property is mutable, we need to make sure that its marker type is open in order to force properties of more general types be assignable to it
            if(!isMutable || currentMarkerType.findAnnotation<DataFrameType>()?.isOpen == true) {
                val markerScheme = getScheme(currentMarkerType, withBaseTypes = true)
                // for mutable properties we do strong typing only at the first processing, after that we allow its type to be more general than actual data frame type
                if (wasProcessedBefore || markerScheme == targetScheme) {
                    // property scheme is valid for current data frame, but we should also check that all compatible open markers are implemented by it
                    val requiredBaseMarkers = markerScheme.getRequiredBaseMarkers().map { it.kclass }
                    if (currentMarkerType.implements(requiredBaseMarkers))
                        return emptyList()
                    // use current marker scheme as a target for generation of new marker interface, so that available properties won't change
                    targetScheme = markerScheme
                }
            }
        }

        // property needs to be recreated. First, try to find existing marker for it
        val declarations = mutableListOf<String>()
        val requiredBaseMarkers = targetScheme.getRequiredBaseMarkers().map { it.kclass }
        val existingMarker = generatedMarkers.firstOrNull {
            isMutable == it.isOpen && it.scheme.equals(targetScheme) && it.kclass.implements(requiredBaseMarkers)
        }
        if (existingMarker != null) {
            markerType = existingMarker.kclass.qualifiedName
        } else {
            markerType = "DataFrameType###"
            declarations.add(getInterfaceDeclaration(targetScheme, markerType, withBaseInterfaces = true, isOpen = isMutable))
        }

        val converter = "\$it.typed<$markerType>()"
        declarations.add(converter)
        return declarations
    }

    private fun getMarkerType(dataFrameType: KType) =
            when (dataFrameType.classifier as? KClass<*>) {
                TypedDataFrame::class -> dataFrameType.arguments[0].type?.classifier as? KClass<*>
                else -> null
            }

    fun generate(df: TypedDataFrame<*>, property: KProperty<*>) = generate(df.df, property)

    fun generate(stub: DataFrameToListNamedStub): List<String> {
        val df = stub.df
        val scheme = df.scheme
        val className = stub.className

        val columnNames = df.cols.map { it.name }
        val classDeclaration = "data class ${className}(" +
                columnNames.map {
                    val field = scheme.byColumn[it]!!
                    "val ${field.fieldName}: ${render(field.fieldType)}"
                }.joinToString() + ")"

        val converter = "\$it.df.rows.map { $className(" +
                columnNames.map {
                    val field = scheme.byColumn[it]!!
                    "it[\"${field.columnName}\"] as ${render(field.fieldType)}"
                }.joinToString() + ")}"

        return listOf(classDeclaration, converter)
    }

    fun generate(stub: DataFrameToListTypedStub): List<String> {
        val df = stub.df
        val dfScheme = df.scheme
        val interfaceScheme = getScheme(stub.interfaze, withBaseTypes = true)
        if (!interfaceScheme.isSuperTo(dfScheme))
            throw Exception()
        val interfaceName = stub.interfaze.simpleName!!
        val interfaceFullName = stub.interfaze.qualifiedName!!
        val implementationName = interfaceName + "Impl"
        val columnNames = interfaceScheme.byColumn.keys.toList()

        val classDeclaration = "data class ${implementationName}(" +
                columnNames.map {
                    val field = dfScheme.byColumn[it]!!
                    "override val ${field.fieldName}: ${render(field.fieldType)}"
                }.joinToString() + ") : $interfaceFullName"

        val converter = "\$it.df.rows.map { $implementationName(" +
                columnNames.map {
                    val field = dfScheme.byColumn[it]!!
                    "it[\"${field.columnName}\"] as ${render(field.fieldType)}"
                }.joinToString() + ")}"

        return listOf(classDeclaration, converter)
    }

    private enum class FieldGenerationMode { declare, override, skip }

    internal fun getInterfaceDeclaration(scheme: Scheme, name: String, withBaseInterfaces: Boolean, isOpen: Boolean): String {

        val markers = mutableListOf<GeneratedMarker>()
        val fields = if (withBaseInterfaces) {
            markers += scheme.getRequiredBaseMarkers().onlyLeafs()
            val generatedFields = computeFieldsGeneration(scheme, markers)

            // try to reduce number of generated fields by implementing some additional interfaces
            val remainedFields = generatedFields.filter { it.second == FieldGenerationMode.declare }.map { it.first }.toMutableList()
            var markersAdded = false

            if (remainedFields.size > 0) {
                val availableMarkers = scheme.getAllBaseMarkers().toMutableList()
                availableMarkers -= markers

                while (remainedFields.size > 0) {
                    val bestMarker = availableMarkers.map { marker -> marker to remainedFields.count { marker.scheme.contains(it) } }.maxBy { it.second }
                    if (bestMarker != null && bestMarker.second > 0) {
                        remainedFields.removeAll { bestMarker.first.scheme.byField[it.fieldName]?.fieldType == it.fieldType }
                        markers += bestMarker.first
                        markersAdded = true
                        availableMarkers -= bestMarker.first
                    } else break
                }
            }
            if (markersAdded) computeFieldsGeneration(scheme, markers) else generatedFields
        } else scheme.values.map { it to FieldGenerationMode.declare }

        val leafMarkers = markers.onlyLeafs()
        val header = "@DataFrameType(isOpen = $isOpen)\ninterface $name"
        val baseInterfacesDeclaration = if (leafMarkers.isNotEmpty()) " : " + leafMarkers.map { it.kclass.qualifiedName!! }.joinToString() else ""
        val fieldsDeclaration = fields.filter { it.second != FieldGenerationMode.skip }.map {
            val field = it.first
            val override = when (it.second) {
                FieldGenerationMode.declare -> ""
                FieldGenerationMode.override -> "override "
                FieldGenerationMode.skip -> throw Exception()
            }
            val columnNameAnnotation = if (field.columnName != field.fieldName) "\t@ColumnName(\"${field.columnName}\")\n" else ""
            val columnTypeAnnotation = if (field.columnType != getColumnType(field.fieldType)) "\t@ColumnType(${render(field.columnType)}::class)\n" else ""
            val valueType = render(field.fieldType)// + if (field.fieldType.isMarkedNullable) "?" else ""
            "${columnNameAnnotation}${columnTypeAnnotation}\t${override}val ${field.fieldName}: $valueType"
        }.joinToString("\n")
        val body = if (fieldsDeclaration.isNotBlank()) "{\n$fieldsDeclaration\n}" else ""
        return header + baseInterfacesDeclaration + body
    }

    private fun computeFieldsGeneration(scheme: Scheme, requiredMarkers: List<GeneratedMarker>): List<Pair<FieldInfo, FieldGenerationMode>> {
        val fields = scheme.values.map { field ->
            val fieldName = field.fieldName
            var generationMode = FieldGenerationMode.declare
            for (baseScheme in requiredMarkers) {
                val baseField = baseScheme.scheme.byField[fieldName]
                if (baseField != null) {
                    generationMode = if (baseField.fieldType == field.fieldType) FieldGenerationMode.skip
                    else if (field.fieldType.isSubtypeOf(baseField.fieldType)) {
                        generationMode = FieldGenerationMode.override
                        break
                    } else throw Exception()
                }
            }
            field to generationMode
        }
        return fields
    }
}

fun <T> TypedDataFrame<T>.generateInterface(name: String? = null, columnSelector: ColumnSelector<T>? = null): String {
    val interfaceName = name ?: "DataEntry"
    val cols = columnSelector?.let { getColumns(it) } ?: columns
    val scheme = CodeGenerator.getScheme(cols)
    return CodeGenerator.getInterfaceDeclaration(scheme, interfaceName, withBaseInterfaces = false, isOpen = true)
}

data class DataFrameToListNamedStub(val df: DataFrame, val className: String)

data class DataFrameToListTypedStub(val df: DataFrame, val interfaze: KClass<*>)

interface Ma {
    val a: Int
}

interface Pa {
    val a: Any
}

interface Chi : Ma, Pa {
    override val a: Int
}