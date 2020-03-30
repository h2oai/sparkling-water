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
package ai.h2o.sparkling.backend.api.h2oframes

import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.SparkContext
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import water.DKV
import water.api.{Handler, HandlerFactory, RestApiContext}
import water.exceptions.H2ONotFoundArgumentException
import water.fvec.Frame

/**
  * Handler for all H2OFrame related queries
  */
class H2OFramesHandler(val sc: SparkContext, val h2oContext: H2OContext) extends Handler {
  def toDataFrame(version: Int, s: DataFrameIDV3): DataFrameIDV3 = {
    val value = DKV.get(s.h2oframe_id)
    if (value == null) {
      throw new H2ONotFoundArgumentException(
        s"H2OFrame with id '${s.h2oframe_id}' does not exist, can not proceed with the transformation!")
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
    if (s.dataframe_id == null) {
      s.dataframe_id = "df_" + dataFrame.rdd.id.toString
    }
    dataFrame.createOrReplaceTempView(s.dataframe_id.toLowerCase)
    SparkSessionUtils.active.sqlContext.cacheTable(s.dataframe_id)
    s
  }
}

object H2OFramesHandler {

  private[api] def registerEndpoints(context: RestApiContext, sc: SparkContext, hc: H2OContext) = {

    val h2oFramesHandler = new H2OFramesHandler(sc, hc)

    def h2oFramesFactory = new HandlerFactory {
      override def create(handler: Class[_ <: Handler]): Handler = h2oFramesHandler
    }

    context.registerEndpoint(
      "getDataFrame",
      "POST",
      "/3/h2oframes/{h2oframe_id}/dataframe",
      classOf[H2OFramesHandler],
      "toDataFrame",
      "Transform H2OFrame with given ID to Spark's DataFrame",
      h2oFramesFactory)
  }
}
