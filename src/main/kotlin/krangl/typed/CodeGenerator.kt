package krangl.typed

import krangl.*
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubtypeOf

@Target(AnnotationTarget.CLASS)
annotation class DataFrameType

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

    data class FieldInfo(val columnName: String, val fieldName: String, val valueType: KType, val columnType: KType) {
        fun isSubFieldOf(other: FieldInfo) =
                columnName == other.columnName && valueType.isSubtypeOf(other.valueType) && columnType.isSubtypeOf(other.columnType)
    }

    internal class Scheme(val fields: Map<String, FieldInfo>) {

        fun isSuperTo(other: Scheme) =
                fields.all {
                    other.fields[it.key]?.isSubFieldOf(it.value) ?: false
                }
    }

    private val generatedTypes = mutableMapOf<Scheme, String>()

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
        scheme.fields.values.asIterable().sortedBy { it.columnName }.forEach { field ->
            val getter = "this[\"${field.columnName}\"]"
            val name = field.fieldName
            val valueType = render(field.valueType)
            val columnType = render(field.columnType)
            declarations.add(codeForProperty(dfTypename, name, columnType, getter))
            declarations.add(codeForProperty(rowTypename, name, valueType, getter))
        }
        return declarations
    }

    private val KClass<*>.scheme: Scheme
        get() = Scheme(declaredMemberProperties.map {
            val columnName = it.findAnnotation<ColumnName>()?.name ?: it.name
            val columnType = it.findAnnotation<ColumnType>()?.type?.createType() ?: getColumnType(it.returnType)
            columnName to FieldInfo(columnName, it.name, it.returnType, columnType)
        }.toMap())

    private fun generateValidFieldName(columnName: String) = columnName.replace(" ", "_")

    internal fun getScheme(columns: Iterable<DataCol>) = Scheme(columns.map { FieldInfo(it.name, generateValidFieldName(it.name), it.valueType, it.javaClass.kotlin.createType()) }
            .map { it.columnName to it }
            .toMap())

    private val DataFrame.scheme: Scheme
        get() = getScheme(cols)

    private fun codeForProperty(typeName: String, name: String, propertyType: String, getter: String): String {
        return "val $typeName.$name: $propertyType get() = ($getter) as $propertyType"
    }

    fun generate(interfaze: KClass<*>): List<String> {
        val fieldSet = interfaze.scheme
        val typeName = interfaze.qualifiedName!!
        val result = generateTypedCode(fieldSet, typeName)
        generatedTypes[fieldSet] = typeName
        return result
    }

    fun generate(df: DataFrame): List<String> {
        val markerType = "DataFrameType###"
        val fieldSet = df.scheme
        val baseTypes = generatedTypes.filter { it.key.isSuperTo(fieldSet) }.values
        val interfaceDefinition = "interface $markerType" + if (baseTypes.isNotEmpty()) {
            " : " + baseTypes.joinToString()
        } else ""
        val declarations = mutableListOf<String>()
        declarations.add(interfaceDefinition)
        declarations.addAll(generateTypedCode(fieldSet, markerType))
        val converter = "\$it.typed<$markerType>()"
        declarations.add(converter)
        return declarations
    }

    fun generate(df: TypedDataFrame<*>) = (df as? TypedDataFrameImpl<*>)
            ?.let {
                generate(it.df)
            } ?: emptyList()

    fun generate(stub: DataFrameToListNamedStub): List<String> {
        val df = stub.df
        val scheme = df.scheme
        val className = stub.className

        val columnNames = df.cols.map { it.name }
        val classDeclaration = "data class ${className}("+
                columnNames.map {
                    val field = scheme.fields.getValue(it)
                    "val ${field.fieldName}: ${render(field.valueType)}"
                }.joinToString() + ")"

        val converter = "\$it.df.rows.map { $className(" +
             columnNames.map {
                 val field = scheme.fields.getValue(it)
                 "it[\"${field.columnName}\"] as ${render(field.valueType)}"
             }.joinToString() + ")}"

        return listOf(classDeclaration, converter)
    }

    fun generate(stub: DataFrameToListTypedStub): List<String> {
        val df = stub.df
        val dfScheme = df.scheme
        val interfaceScheme = stub.interfaze.scheme
        if(!interfaceScheme.isSuperTo(dfScheme))
            throw Exception()
        val interfaceName = stub.interfaze.simpleName!!
        val interfaceFullName = stub.interfaze.qualifiedName!!
        val implementationName = interfaceName + "Impl"
        val columnNames = interfaceScheme.fields.keys.toList()

        val classDeclaration = "data class ${implementationName}("+
                columnNames.map {
                    val field = dfScheme.fields.getValue(it)
                    "override val ${field.fieldName}: ${render(field.valueType)}"
                }.joinToString() + ") : $interfaceFullName"

        val converter = "\$it.df.rows.map { $implementationName(" +
                columnNames.map {
                    val field = dfScheme.fields.getValue(it)
                    "it[\"${field.columnName}\"] as ${render(field.valueType)}"
                }.joinToString() + ")}"

        return listOf(classDeclaration, converter)
    }
}

fun <T> TypedDataFrame<T>.getInterface(name: String? = null, columnSelector: ColumnSelector<T>? = null): String {
    val interfaceName = name ?: "DataEntry"
    val cols = columnSelector?.let {getColumns(it)} ?: columns
    val scheme = CodeGenerator.getScheme(cols)
    (IntCol::class.createType().classifier as KClass<*>).simpleName
    return "@DataFrameType\ninterface $interfaceName {\n" +
            scheme.fields.values.map {
                val columnNameAnnotation = if (it.columnName != it.fieldName) "\t@ColumnName(\"${it.columnName}\")\n" else ""
                val columnTypeAnnotation = if (it.columnType != CodeGenerator.getColumnType(it.valueType)) "\t@ColumnType(${(it.columnType.classifier as KClass<*>).simpleName}::class)\n" else ""
                val valueType = (it.valueType.classifier as KClass<*>).simpleName + if (it.valueType.isMarkedNullable) "?" else ""
                "${columnNameAnnotation}${columnTypeAnnotation}\tval ${it.fieldName}: $valueType"
            }.joinToString("\n") + "\n}"
}

data class DataFrameToListNamedStub(val df: DataFrame, val className: String)

data class DataFrameToListTypedStub(val df: DataFrame, val interfaze: KClass<*>)