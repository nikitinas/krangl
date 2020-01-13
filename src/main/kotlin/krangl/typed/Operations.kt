package krangl.typed

import krangl.DataCol
import krangl.DataFrame

class TypedColumnsFromDataRowBuilder<T>(val dataFrame: TypedDataFrame<T>) {
    internal val columns = mutableListOf<DataCol>()

    fun add(column: DataCol) = columns.add(column)

    inline fun <reified R> add(name: String, noinline expression: TypedDataFrameRow<T>.() -> R?) = add(dataFrame.new(name, expression))

    inline infix fun <reified R> String.to(noinline expression: TypedDataFrameRow<T>.() -> R?) = add(this, expression)

    inline infix fun <reified R> String.`=`(noinline expression: TypedDataFrameRow<T>.() -> R?) = add(this, expression)

    inline infix operator fun <reified R> String.invoke(noinline expression: TypedDataFrameRow<T>.() -> R?) = add(this, expression)
}

class SummarizeDataFrameBuilder<T>(val dataFrame: TypedDataFrame<T>) {
    internal val columns = mutableListOf<DataCol>()

    fun add(column: DataCol) = columns.add(column)

    class Aggregator<T,D>(selector: TypedDataFrame<T>.()->D, operation: (accumulator: D,value: D)->D) {
        class Value<T, D>(owner: Aggregator<T, D>)
        class ResultSelector<T, D>(owner: Aggregator<T, D>, selector: TypedDataFrame<T>.() -> D)
    }

    inline fun <reified D: Comparable<D>> maxBy(selector: TypedDataFrameRow<T>.()->D){
        dataFrame.groups().map{it.rows.maxBy { selector(it) }}
    }

    inline fun <reified R> add(name: String, noinline expression: TypedDataFrame<T>.() -> R?) = add(newColumn(name, dataFrame.groups().map{expression(it)}))

    inline infix fun <reified R> String.to(noinline expression: TypedDataFrame<T>.() -> R?) = add(this, expression)

    inline infix fun <reified R> String.`=`(noinline expression: TypedDataFrame<T>.() -> R?) = add(this, expression)

    inline infix operator fun <reified R> String.invoke(noinline expression: TypedDataFrame<T>.() -> R?) = add(this, expression)
}

// add Column

operator fun DataFrame.plus(col: DataCol) = dataFrameOf(cols + col)
operator fun DataFrame.plus(col: Iterable<DataCol>) = dataFrameOf(cols + col)
operator fun <T> TypedDataFrame<T>.plus(col: DataCol) = (df + col).typed<T>()
operator fun <T> TypedDataFrame<T>.plus(col: Iterable<DataCol>) = dataFrameOf(df.cols + col).typed<T>()

inline fun <reified T, D> TypedDataFrame<D>.add(name: String, noinline expression: TypedDataFrameRow<D>.() -> T?) =
        (this + new(name, expression)).setDirtyScheme()

inline fun <reified T> DataFrame.addColumn(name: String, values: List<T?>) =
        this + newColumn(name, values)

fun DataFrame.addColumn(name: String, col: DataCol) =
        this + col.rename(name)

fun <T> TypedDataFrame<T>.add(body: TypedColumnsFromDataRowBuilder<T>.() -> Unit): TypedDataFrame<T> {
    val builder = TypedColumnsFromDataRowBuilder(this)
    body(builder)
    return dataFrameOf(columns + builder.columns).typed<T>(isTypeDirty = true)
}

// map (transmute)

fun <T> TypedDataFrame<T>.map(body: TypedColumnsFromDataRowBuilder<T>.() -> Unit): DataFrame {
    val builder = TypedColumnsFromDataRowBuilder(this)
    body(builder)
    return dataFrameOf(builder.columns)
}

// filter

fun <T> TypedDataFrame<T>.filter(predicate: TypedDataFrameRow<T>.() -> Boolean) =
        df.filter {
            rowWise { getRow ->
                BooleanArray(nrow) { index ->
                    predicate(getRow(index)!!)
                }
            }
        }.typed<T>()


// summarize

fun <T> TypedDataFrame<T>.summarize(body: SummarizeDataFrameBuilder<T>.()->Unit):TypedDataFrame<T> {
    val builder = SummarizeDataFrameBuilder(this)
    body(builder)
    return (this.groupedBy() + builder.columns).typed<T>(isTypeDirty = true)
}