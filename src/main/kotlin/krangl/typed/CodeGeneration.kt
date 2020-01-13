package krangl.typed

import krangl.*
import kotlin.reflect.KClass
import kotlin.reflect.full.declaredMemberProperties

annotation class DataFrameType

fun TypedDataFrame<*>.generateTypedCode() = (this as? TypedDataFrameImpl<*>)
        ?.let {
            if(it.isTypeDirty) df.generateTypedCode()
            else null
        } ?: emptyList()

fun getColumnType(clazz: Class<*>) =
    when(clazz) {
        Int::class.java -> IntCol::class.java
        Double::class.java -> DoubleCol::class.java
        Boolean::class.java -> BooleanCol::class.java
        String::class.java -> StringCol::class.java
        else -> AnyCol::class.java
    }

class FieldSet(val fields: Map<String, KClass<*>>){

    fun isSubsetOf(other: FieldSet)=
        fields.all {
            other.fields[it.key]?.equals(it.value) ?: false
        }
}

val generatedTypes = mutableMapOf<FieldSet, String>()

fun generateTypedCode(fieldSet: FieldSet, markerType: String): List<String> {
    val declarations = mutableListOf<String>()
    val dfTypename = TypedDataFrame::class.java.name + "<$markerType>"
    val rowTypename = TypedDataFrameRow::class.java.name + "<$markerType>"
    fieldSet.fields.asIterable().sortedBy { it.key }.forEach { col ->
        val name = col.key
        val getter = "this[\"${name}\"]"
        val valueType = col.value.qualifiedName!!
        val columnType = getColumnType(col.value.java).name
        declarations.add(codeForProperty(dfTypename, name, columnType, getter))
        declarations.add(codeForProperty(rowTypename, name, valueType, getter))
    }
    return declarations
}

val KClass<*>.fieldSet: FieldSet get() = FieldSet(declaredMemberProperties.map{
    it.name to it.returnType.classifier as KClass<*>
}.toMap())

fun generateTypedCode(clazz: KClass<*>): List<String> {
    val fieldSet = clazz.fieldSet
    val typeName = clazz.qualifiedName!!
    val result = generateTypedCode(fieldSet, typeName)
    generatedTypes[fieldSet] = typeName
    return result
}

val DataFrame.fieldSet: FieldSet get() = FieldSet(cols.map {it.name to it.valueType}.toMap())

fun DataFrame.generateTypedCode() : List<String> {
    val markerType = "DataFrameMarker###"
    val fieldSet = fieldSet
    val baseTypes = generatedTypes.filter { it.key.isSubsetOf(fieldSet) }.values
    val classDefinition = "interface $markerType" + if(baseTypes.isNotEmpty()) {" : " + baseTypes.joinToString()} else ""
    val declarations = mutableListOf<String>()
    declarations.add(classDefinition)
    declarations.addAll(generateTypedCode(fieldSet, markerType))
    val converter = "\$it.typed<$markerType>()"
    declarations.add(converter)
    return declarations
}

fun codeForProperty(typeName: String, name: String, propertyType: String, getter: String): String {
    return "val $typeName.$name: $propertyType get() = ($getter) as $propertyType"
}