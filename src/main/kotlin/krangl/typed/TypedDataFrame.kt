package krangl.typed

import krangl.DataCol
import krangl.DataFrame
import krangl.DataFrameRow
import krangl.util.popSafe
import java.util.*

interface DataFrameRowEx<out T> {
    val prev: DataFrameRowEx<T>?
    val next: DataFrameRowEx<T>?
    val index: Int
    fun getRow(index: Int): DataFrameRowEx<T>?
    operator fun get(name: String): Any?
}

interface TypedDataFrame<out T> {
    val df: DataFrame
    val nrow: Int get() = df.nrow
    val columns: List<DataCol> get() = df.cols
    val rows: Iterable<DataFrameRowEx<T>>
    val isTypeDirty: Boolean

    operator fun get(rowIndex: Int): DataFrameRowEx<T>
    operator fun get(columnName: String): DataCol = df[columnName]

    fun select(columns: Iterable<DataCol>) = df.select(columns.map { it.name }).typed<T>(isTypeDirty = true)
    fun select(vararg columns: DataCol) = select(columns.toList())
    fun select(vararg selectors: TypedDataFrame<T>.()-> DataCol) = select(selectors.map{it(this)})

    fun sortedBy(columns: Iterable<DataCol>) = df.sortedBy(*(columns.map { it.name }.toTypedArray())).typed<T>(isTypeDirty)
    fun sortedBy(vararg columns: DataCol) = sortedBy(columns.toList())
    fun sortedBy(vararg selectors: TypedDataFrame<T>.() -> DataCol) = sortedBy(selectors.map{it(this)})
    fun sortedBy(selector: TypedDataFrame<T>.() -> DataCol) = sortedBy(*arrayOf(selector))

    fun sortedByDesc(columns: Iterable<DataCol>) = df.sortedByDescending(*(columns.map { it.name }.toTypedArray())).typed<T>(isTypeDirty)
    fun sortedByDesc(vararg columns: DataCol) = sortedByDesc(columns.toList())
    fun sortedByDesc(vararg selectors: TypedDataFrame<T>.() -> DataCol) = sortedByDesc(selectors.map{it(this)})
    fun sortedByDesc(selector: TypedDataFrame<T>.() -> DataCol) = sortedByDesc(*arrayOf(selector))

    fun remove(cols: Iterable<DataCol>) = df.remove(cols.map { it.name }).typed<T>(isTypeDirty = true)
    fun remove(vararg cols: DataCol) = remove(cols.toList())
    fun remove(vararg selectors: TypedDataFrame<T>.()-> DataCol) = remove(selectors.map{it(this)})
    fun remove(selector: TypedDataFrame<T>.()-> DataCol) = remove(*arrayOf(selector))

    fun groupBy(cols: Iterable<DataCol>) = df.groupBy(*(cols.map{it.name}.toTypedArray())).typed<T>(isTypeDirty)
    fun groupBy(vararg cols: DataCol) = groupBy(cols.toList())
    fun groupBy(vararg selectors: TypedDataFrame<T>.()-> DataCol) = groupBy(selectors.map{it(this)})
    fun groupBy(selector: TypedDataFrame<T>.()-> DataCol) = groupBy(*arrayOf(selector))

    fun ungroup() = df.ungroup().typed<T>(isTypeDirty)

    fun groupedBy() = df.groupedBy().typed<T>(isTypeDirty = true)
    fun groups() = df.groups().map { it.typed<T>(isTypeDirty) }
}

internal class TypedDataFrameImpl<T>(override val df: DataFrame, override val isTypeDirty: Boolean = false) : TypedDataFrame<T> {
    private val rowResolver = RowResolver<T>(df)

    override val rows = object : Iterable<DataFrameRowEx<T>> {
        override fun iterator() =

                object : Iterator<DataFrameRowEx<T>> {
                    var curRow = 0

                    val resolver = RowResolver<T>(df)

                    override fun hasNext(): Boolean = curRow < nrow

                    override fun next(): DataFrameRowEx<T> = resolver[curRow++]!!
                }
    }
    override fun get(rowIndex: Int) = rowResolver[rowIndex]!!
}

internal class DataFrameRowExImpl<T>(var row: DataFrameRow, override var index: Int, val resolver: RowResolver<T>) : DataFrameRowEx<T> {

    override operator fun get(name: String): Any? = row[name]

    override val prev: DataFrameRowEx<T>?
        get() = resolver[index - 1]
    override val next: DataFrameRowEx<T>?
        get() = resolver[index + 1]

    override fun getRow(index: Int): DataFrameRowEx<T>? = resolver[index]
}

internal class RowResolver<T>(private val df: DataFrame) {
    private val pool = LinkedList<DataFrameRowExImpl<T>>()
    private val map = mutableMapOf<Int, DataFrameRowExImpl<T>>()

    fun resetMapping() {
        pool.addAll(map.values)
        map.clear()
    }

    operator fun get(index: Int): DataFrameRowEx<T>? =
            if (index < 0 || index >= df.nrow) null
            else map[index] ?: pool.popSafe()?.also {
                it.row = df.row(index)
                it.index = index
                map[index] = it
            } ?: DataFrameRowExImpl(df.row(index), index, this).also { map[index] = it }

}

fun <T, D> TypedDataFrame<D>.rowWise(body: ((Int) -> DataFrameRowEx<D>?) -> T): T {
    val resolver = RowResolver<D>(this.df)
    fun getRow(index: Int): DataFrameRowEx<D>? {
        resolver.resetMapping()
        return resolver[index]
    }
    return body(::getRow)
}

fun <T> DataFrame.typed(isTypeDirty: Boolean = false): TypedDataFrame<T> = TypedDataFrameImpl<T>(this, isTypeDirty)

fun <T> TypedDataFrame<*>.typed(isTypeDirty: Boolean = false) = df.typed<T>(isTypeDirty)

fun <T> TypedDataFrame<T>.setDirtyScheme() = df.typed<T>(isTypeDirty = true)
