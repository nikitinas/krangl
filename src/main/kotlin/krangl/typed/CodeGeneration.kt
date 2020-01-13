package krangl.typed

import krangl.DataFrame

fun TypedDataFrame<*>.generateTypedCode() = (this as? TypedDataFrameImpl<*>)
        ?.let {
            if(it.isTypeDirty) df.generateTypedCode()
            else null
        } ?: emptyList()

fun DataFrame.generateTypedCode(): List<String> {
    val markerType = "DataFrameMarker###"
    val classDefinition = "class $markerType"
    val declarations = mutableListOf(classDefinition)
    val dfTypename = TypedDataFrame::class.java.name + "<$markerType>"
    val rowTypename = DataFrameRowEx::class.java.name + "<$markerType>"
    cols.sortedBy { it.name }.forEach { col ->
        val getter = "this[\"${col.name}\"]"
        val columnType = col.javaClass.name
        val valueType = col.valueType.qualifiedName!!
        declarations.add(codeForProperty(dfTypename, col.name, columnType, getter))
        declarations.add(codeForProperty(rowTypename, col.name, valueType, getter))
    }
    val converter = "\$it.typed<$markerType>()"
    declarations.add(converter)
    return declarations
}

fun codeForProperty(typeName: String, name: String, propertyType: String, getter: String): String {
    return "val $typeName.$name: $propertyType get() = ($getter) as $propertyType"
}