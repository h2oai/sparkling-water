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
package water.api.DataFrames

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import water.Iced
import water.api.Handler
import water.exceptions.H2ONotFoundArgumentException

/**
 * Handler for all Spark's DataFrame related queries
 */
class DataFramesHandler(val sc: SparkContext, val h2oContext: H2OContext) extends Handler {
  val sqlContext = SparkSession.builder().getOrCreate().sqlContext

  def list(version: Int, s: DataFramesV3): DataFramesV3 = {
    val r = s.createAndFillImpl()
    r.dataframes = fetchAll()
    s.fillFromImpl(r)
    s
  }

  def fetchAll(): Array[IcedDataFrameInfo] = {
    val names = sqlContext.tableNames()
    names.map(name => new IcedDataFrameInfo(name, sqlContext.table(name).rdd.partitions.length, sqlContext.table(name).schema.json))
  }

  def getDataFrame(version: Int, s: DataFrameV3): DataFrameV3 = {
    if(!sqlContext.tableNames().toList.contains(s.dataframe_id)){
      throw new H2ONotFoundArgumentException(s"DataFrame with id '${s.dataframe_id}' does not exist!")
    }
    val dataFrame = sqlContext.table(s.dataframe_id)
    s.schema = dataFrame.schema.json
    s.partitions = dataFrame.rdd.partitions.length
    s
  }

  // TODO(vlad): see the same code in RDDsHandler
  def toH2OFrame(version: Int, s: H2OFrameIDV3): H2OFrameIDV3 = {
    if(!sqlContext.tableNames().toList.contains(s.dataframe_id)){
      throw new H2ONotFoundArgumentException(s"DataFrame with id '${s.dataframe_id}' does not exist, can not proceed with the transformation!")
    }
    val dataFrame: DataFrame = sqlContext.table(s.dataframe_id)
    val h2oFrame = if( s.h2oframe_id == null ) h2oContext.asH2OFrame(dataFrame) else h2oContext.asH2OFrame(dataFrame,s.h2oframe_id.toLowerCase())
    s.h2oframe_id = h2oFrame._key.toString
    s
  }
}

/** Simple POJO holding list of DataFrames */
private[api] class DataFrames extends Iced[DataFrames] {
  var dataframes: Array[IcedDataFrameInfo] = _
}

private[api] class IcedDataFrameInfo(val dataframe_id: String, val partitions: Int, val schema: String) extends Iced[IcedDataFrameInfo] {
  def this() = this(null, -1, null) // initialize with empty values, this is used by the createImpl method in the
  //RequestServer, as it calls constructor without any arguments
}


private[api] class IcedH2OFrameID(val dataframe_id: String, val h2oframe_id: String) extends Iced[IcedH2OFrameID] {

  def this() = this(null, null) // initialize with empty values, this is used by the createImpl method in the
  //RequestServer, as it calls constructor without any arguments
}
