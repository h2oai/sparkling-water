package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.backend.{H2OAwareRDD, H2OFrameRelation, Writer, WriterMetadata}
import ai.h2o.sparkling.ml.utils.SchemaUtils._
import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.{H2OContext, H2OFrame, SparkTimeZone}
import org.apache.spark.expose.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

object SparkDataFrameConverter extends Logging {

  /**
    * Create a Spark DataFrame from a given REST-based H2O frame.
    *
    * @param hc           an instance of H2O context
    * @param fr           an instance of H2O frame
    * @param copyMetadata copy H2O metadata into Spark DataFrame
    * @return a new DataFrame definition using given H2OFrame as data source
    */
  def toDataFrame(hc: H2OContext, fr: H2OFrame, copyMetadata: Boolean): DataFrame = {
    val spark = SparkSessionUtils.active
    val relation = H2OFrameRelation(fr, copyMetadata)(spark.sqlContext)
    spark.baseRelationToDataFrame(relation)
  }

  def toH2OFrame(
      hc: H2OContext,
      dataFrame: DataFrame,
      frameKeyName: Option[String] = None,
      featureColsForConstCheck: Option[Seq[String]] = None): H2OFrame = {
    val df = dataFrame.toDF() // Because of PySparkling, we can receive Dataset[Primitive] in this method, ensure that
    // we are dealing with Dataset[Row]
    val flatDataFrame = flattenDataFrame(df)
    val schema = flatDataFrame.schema
    val rdd = flatDataFrame.rdd
    if (hc.getConf.runsInInternalClusterMode) {
      rdd.persist(StorageLevel.DISK_ONLY)
    } else {
      rdd.persist()
    }

    val elemMaxSizes = collectMaxElementSizes(rdd, schema)
    val vecIndices = collectVectorLikeTypes(schema).toArray
    val flattenedSchema = expandedSchema(schema, elemMaxSizes)
    val h2oColNames = flattenedSchema.map(field => "\"" + field.name + "\"").toArray
    val maxVecSizes = vecIndices.map(elemMaxSizes(_))

    val expectedTypes = DataTypeConverter.determineExpectedTypes(schema)

    val uniqueFrameId = frameKeyName.getOrElse("frame_rdd_" + rdd.id + scala.util.Random.nextInt())
    val metadata =
      WriterMetadata(
        hc.getConf,
        uniqueFrameId,
        expectedTypes,
        maxVecSizes,
        SparkTimeZone.current(),
        featureColsForConstCheck)
    val result = Writer.convert(new H2OAwareRDD(hc.getH2ONodes(), rdd), h2oColNames, metadata)
    rdd.unpersist(blocking = false)
    result
  }

}
