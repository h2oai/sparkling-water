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
import org.apache.spark.sql.{DataFrame, SQLContext}
import water.Iced
import water.api.Handler

/**
 * DataFrames handler.
 */
class DataFramesHandler(val sc: SparkContext, val h2OContext: H2OContext) extends Handler {
  val sqlContext = SQLContext.getOrCreate(sc)

  def list(version: Int, s: DataFramesV3): DataFramesV3 = {
    val r = s.createAndFillImpl()
    r.dataFrames = fetchAll()
    s.fillFromImpl(r)
    s
  }

  def fetchAll(): Array[IcedDataFrameInfo] = {
    val names = sqlContext.tableNames()
    names.map(name => new IcedDataFrameInfo(name, sqlContext.table(name).schema.json))
  }

  def getDataFrame(version: Int, s: DataFrameWithMsgV3): DataFrameWithMsgV3 = {
    val r = s.createAndFillImpl()
    if(!sqlContext.tableNames().toList.contains(s.searched_dataframe_id)){
      s.msg = s"DataFrame with id '${s.searched_dataframe_id}' does not exist"
    }else{
      val dataFrame = sqlContext.table(s.searched_dataframe_id)
      r.dataframe = new IcedDataFrameInfo(s.searched_dataframe_id, dataFrame.schema.json)
      s.fillFromImpl(r)
      s.msg = "OK"
    }
    s
  }

  /**
   * Transform to H2OFrame end return ID of the frame
   * @param version
   * @param s
   * @return
   */
  def toH2OFrame(version: Int, s: H2OFrameIDV3): H2OFrameIDV3 = {
    if(!sqlContext.tableNames().toList.contains(s.dataframe_id)){
      s.h2oframe_id=""
      s.msg = s"DataFrame with id '${s.dataframe_id}' does not exist, can not proceed with the transformation"
    }else{
      val dataFrame: DataFrame = sqlContext.table(s.dataframe_id)
      val h2oFrame = h2OContext.asH2OFrame(dataFrame)
      s.h2oframe_id = h2oFrame._key.toString
      s.msg="Success"
    }
    s
  }
}

/** Simple implementation pojo holding list of DataFrames */
private[api] class DataFrames extends Iced[DataFrames] {
  var dataFrames: Array[IcedDataFrameInfo] = _
}

private[api] class IcedDataFrameInfo(val dataframe_id: String, val schema: String) extends Iced[IcedDataFrameInfo] {
  def this() = this("df_-1", "{}") // initialize with empty values, this is used by the createImpl method in the
  //RequestServer, as it calls constructor without any arguments
}

private[api] class IcedDataFrameWithMsgInfo() extends Iced[IcedDataFrameWithMsgInfo]{
  var dataframe: IcedDataFrameInfo = _
}


private[api] class IcedH2OFrameID(val dataframe_id: String) extends Iced[IcedH2OFrameID] {

  def this() = this("") // initialize with empty values, this is used by the createImpl method in the
  //RequestServer, as it calls constructor without any arguments
}
