/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.h2o

import ai.h2o.sparkling.backend.H2OFrameRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import water.DKV

/**
  * Provides access to H2OFrame from pure SQL statements (i.e. for users of the
  * JDBC server).
  */
class DefaultSource extends RelationProvider
  with SchemaRelationProvider with CreatableRelationProvider with DataSourceRegister {

  /**
    * Short alias for spark-csv data source.
    */
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
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
    * Creates a new relation for data store in H2OFrame given parameters.
    * Parameters have to include 'key'
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): H2OFrameRelation = {
    val key = checkKey(parameters)

    H2OFrameRelation(ai.h2o.sparkling.frame.H2OFrame(key), copyMetadata = true)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val key = checkKey(parameters)
    val originalFrame = DKV.getGet[Frame](key)
    implicit val h2oContext: H2OContext =
      H2OContext.ensure("H2OContext has to be started in order to save/load data using H2O Data source.")

    if (originalFrame != null) {
      mode match {
        case SaveMode.Append =>
          sys.error("Appending to H2O Frame is not supported.")
        case SaveMode.Overwrite =>
          originalFrame.remove()
          h2oContext.asH2OFrame(data, key)
        case SaveMode.ErrorIfExists =>
          sys.error(s"Frame with key '$key' already exists, if you want to override it set the save mode to override.")
        case SaveMode.Ignore => // do nothing
      }
    } else {
      // save as H2O Frame
      h2oContext.asH2OFrame(data, key)
    }

    createRelation(sqlContext, parameters, data.schema)
  }
}
