package ai.h2o.sparkling

import ai.h2o.sparkling.backend.H2OFrameRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Provides access to H2OFrame from pure SQL statements (i.e. for users of the JDBC server).
  */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with DataSourceRegister {

  override def shortName(): String = "h2o"

  private def checkKey(parameters: Map[String, String]): String = {
    // 'path' option is alias for frame key. It is used so we can call:
    // sqlContext.read.format("h2o").option("key",key).load()  - this is using key option
    // sqlContext.read.format("h2o").load(key)  - this is using path option, but it's nicer to use it like this

    // if both are set, the 'key' option is chosen
    parameters.getOrElse("key", parameters.getOrElse("path", sys.error("'key' must be specified for H2O Frame.")))
  }

  /**
    * Creates a new relation for data store in H2OFrame given parameters.
    * Parameters have to include 'key'
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
    * Creates a new relation for data store in H2OFrame given parameters.
    * Parameters have to include 'key'
    */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): H2OFrameRelation = {
    val key = checkKey(parameters)

    H2OFrameRelation(ai.h2o.sparkling.H2OFrame(key), copyMetadata = true)(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val key = checkKey(parameters)
    val hc = H2OContext.ensure("H2OContext has to be started in order to save/load data using H2O Data source.")

    if (H2OFrame.exists(key)) {
      val originalFrame = H2OFrame(key)
      mode match {
        case SaveMode.Append =>
          sys.error("Appending to H2O Frame is not supported.")
        case SaveMode.Overwrite =>
          originalFrame.delete()
          hc.asH2OFrame(data, key)
        case SaveMode.ErrorIfExists =>
          sys.error(s"Frame with key '$key' already exists, if you want to override it set the save mode to override.")
        case SaveMode.Ignore => // do nothing
      }
    } else {
      hc.asH2OFrame(data, key)
    }
    createRelation(sqlContext, parameters, data.schema)
  }
}
