package ai.h2o.sparkling.backend

import ai.h2o.sparkling.backend.utils.SupportedTypes._
import ai.h2o.sparkling.{H2OContext, H2OFrame, SparkTimeZone}
import ai.h2o.sparkling.backend.converters.DataTypeConverter
import ai.h2o.sparkling.backend.utils.ReflectionUtils
import ai.h2o.sparkling.extensions.serde.ExpectedTypes.ExpectedType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.{Partition, TaskContext}

/**
  * H2O H2OFrame wrapper providing RDD[Row]=DataFrame API.
  *
  * @param frame           frame which will be wrapped as DataFrame
  * @param requiredColumns list of the columns which should be provided by iterator, null means all
  * @param hc              an instance of H2O Context
  */
private[backend] class H2ODataFrame(val frame: H2OFrame, val requiredColumns: Array[String])(
    @transient val hc: H2OContext)
  extends H2OAwareEmptyRDD[InternalRow](hc.sparkContext, hc.getH2ONodes())
  with H2OSparkEntity {

  private val h2oConf = hc.getConf
  private val sparkTimeZone = SparkTimeZone.current()

  def this(frame: H2OFrame)(@transient hc: H2OContext) = this(frame, null)(hc)

  private val colNames = frame.columns.map(_.name)

  protected val types: Array[DataType] = frame.columns.map(ReflectionUtils.dataTypeFor)

  override val selectedColumnIndices: Array[Int] = {
    if (requiredColumns == null) {
      colNames.indices.toArray
    } else {
      requiredColumns.map(colNames.indexOf)
    }
  }

  override val expectedTypes: Array[ExpectedType] = {
    val javaClasses = selectedColumnIndices.map(indexToSupportedType(_).javaClass)
    DataTypeConverter.expectedTypesFromClasses(javaClasses)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    new H2OChunkIterator[InternalRow] {
      private val chnk = frame.chunks.find(_.index == split.index).head
      override lazy val reader: Reader = new Reader(
        frameId,
        split.index,
        chnk.numberOfRows,
        chnk.location,
        expectedTypes,
        selectedColumnIndices,
        h2oConf,
        sparkTimeZone)

      private lazy val columnIndicesWithTypes: Array[(Int, SimpleType[_])] = {
        selectedColumnIndices.map(i => (i, bySparkType(types(i))))
      }

      private lazy val columnValueProviders: Array[() => Option[Any]] = {
        for {
          (columnIndex, supportedType) <- columnIndicesWithTypes
          valueReader = reader.OptionReaders(byBaseType(supportedType))
          provider = () => valueReader.apply(columnIndex)
        } yield provider
      }

      def readOptionalData: Seq[Option[Any]] = columnValueProviders.map(_())

      private def readRow: InternalRow = {
        val optionalData: Seq[Option[Any]] = readOptionalData
        val nullableData: Seq[Any] = optionalData.map(_.orNull)
        InternalRow.fromSeq(nullableData)
      }

      override def next(): InternalRow = {
        val row = readRow
        reader.increaseRowIdx()
        row
      }
    }
  }

  protected def indexToSupportedType(index: Int): SupportedType = {
    val column = frame.columns(index)
    ReflectionUtils.supportedType(column)
  }
}
