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
package water.api.H2OFrames

import org.apache.spark.SparkContext
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.sql.SQLContext
import water.api.Handler
import water.exceptions.H2ONotFoundArgumentException
import water.fvec.Frame
import water.{DKV, Iced}

/**
 * Handler for all H2OFrame related queries
 */
class H2OFramesHandler(val sc: SparkContext, val h2oContext: H2OContext) extends Handler {
  implicit val sqlContext = SQLContext.getOrCreate(sc)

  def toDataFrame(version: Int, s: DataFrameIDV3): DataFrameIDV3 = {
    val value = DKV.get(s.h2oframe_id)
    if(value == null){
      throw new H2ONotFoundArgumentException(s"H2OFrame with id '${s.h2oframe_id}' does not exist, can not proceed with the transformation!")
    }

    val h2oFrame: H2OFrame = value.className() match {
      case name if name.equals(classOf[Frame].getName) => {
        import h2oContext.implicits._
        value.get[Frame]()
      }
      case name if name.equals(classOf[H2OFrame].getName) => value.get[H2OFrame]()
    }

    val dataFrame = h2oContext.asDataFrame(h2oFrame)
    dataFrame.rdd.cache()
    if(s.dataframe_id == null){
      s.dataframe_id = "df_" + dataFrame.rdd.id.toString
    }
    dataFrame.registerTempTable(s.dataframe_id.toLowerCase)
    sqlContext.cacheTable(s.dataframe_id)
    s
  }

}

private[api] class IcedDataFrameID(val h2oframe_id: String, val dataframe_id: String) extends Iced[IcedDataFrameID] {

  def this() = this(null, null) // initialize with empty values, this is used by the createImpl method in the
  //RequestServer, as it calls constructor without any arguments
}
