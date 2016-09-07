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

package water.api

import java.util.ServiceLoader

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import water.api.DataFrames.DataFramesHandler
import water.api.H2OFrames.H2OFramesHandler
import water.api.RDDs.RDDsHandler
import water.api.scalaInt.ScalaCodeHandler

trait RestApi {
  def register(h2oContext: H2OContext): Unit
}

private[api] class RestAPIManager(hc: H2OContext) {
  private val loader: ServiceLoader[RestApi] = ServiceLoader.load(classOf[RestApi])

  def registerAll(): Unit = {
    // Register first the core
    register(CoreRestApi)
    // Then additional APIs
    import scala.collection.JavaConversions._
    loader.reload()
    loader.foreach(api => register(api))
  }

  def register(api: RestApi): Unit = {
    api.register(hc)
  }
}

object RestAPIManager {
  def apply(hc: H2OContext) = new RestAPIManager(hc)
}

private object CoreRestApi extends RestApi {

  def register(h2oContext: H2OContext): Unit = {
    if(h2oContext.getConf.isH2OReplEnabled){
      registerScalaIntEndp(h2oContext.sparkContext, h2oContext)
    }
    registerDataFramesEndp(h2oContext.sparkContext, h2oContext)
    registerH2OFramesEndp(h2oContext.sparkContext, h2oContext)
    registerRDDsEndp(h2oContext.sparkContext, h2oContext)
  }

  private def registerH2OFramesEndp(sc: SparkContext, h2oContext: H2OContext) = {

    val h2oFramesHandler = new H2OFramesHandler(sc, h2oContext)

    def h2oFramesFactory = new HandlerFactory {
      override def create(handler: Class[_ <: Handler]): Handler = h2oFramesHandler
    }

    RequestServer.registerEndpoint("getDataFrame", "POST", "/3/h2oframes/{h2oframe_id}/dataframe",
                                   classOf[H2OFramesHandler], "toDataFrame", "Transform H2OFrame with given ID to Spark's DataFrame",
                                   h2oFramesFactory)

  }

  private def registerRDDsEndp(sc: SparkContext, h2oContext: H2OContext) = {

    val rddsHandler = new RDDsHandler(sc, h2oContext)

    def rddsFactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = rddsHandler
    }
    RequestServer.registerEndpoint("listRDDs", "GET", "/3/RDDs", classOf[RDDsHandler], "list",
                                   "Return all RDDs within Spark cloud", rddsFactory)

    RequestServer.registerEndpoint("getRDD", "POST", "/3/RDDs/{rdd_id}", classOf[RDDsHandler],
                                   "getRDD", "Get RDD with the given ID from Spark cloud", rddsFactory)

    RequestServer.registerEndpoint("rddToH2OFrame", "POST", "/3/RDDs/{rdd_id}/h2oframe",
                                   classOf[RDDsHandler], "toH2OFrame", "Transform RDD with the given ID to H2OFrame", rddsFactory)

  }

  private def registerDataFramesEndp(sc: SparkContext, h2oContext: H2OContext) = {

    val dataFramesHandler = new DataFramesHandler(sc, h2oContext)

    def dataFramesfactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = dataFramesHandler
    }

    RequestServer.registerEndpoint("listDataFrames", "GET", "/3/dataframes",
                                   classOf[DataFramesHandler], "list", "Return all Spark's DataFrames", dataFramesfactory)

    RequestServer.registerEndpoint("getDataFrame", "POST", "/3/dataframes/{dataframe_id}",
                                   classOf[DataFramesHandler], "getDataFrame", "Get Spark's DataFrame with the given ID", dataFramesfactory)

    RequestServer.registerEndpoint("dataFrametoH2OFrame", "POST",
                                   "/3/dataframes/{dataframe_id}/h2oframe", classOf[DataFramesHandler], "toH2OFrame",
                                   "Transform Spark's DataFrame with the given ID to H2OFrame", dataFramesfactory)

  }

  private def registerScalaIntEndp(sc: SparkContext, h2oContext: H2OContext) = {
    val scalaCodeHandler = new ScalaCodeHandler(sc, h2oContext)
    def scalaCodeFactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = scalaCodeHandler
    }
    RequestServer.registerEndpoint("interpretScalaCode", "POST" ,"/3/scalaint/{session_id}",
                                   classOf[ScalaCodeHandler], "interpret", "Interpret the code and return the result",
                                   scalaCodeFactory)

    RequestServer.registerEndpoint("initScalaSession", "POST", "/3/scalaint",
                                   classOf[ScalaCodeHandler], "initSession", "Return session id for communication with scala interpreter",
                                   scalaCodeFactory)

    RequestServer.registerEndpoint("getScalaSessions", "GET" ,"/3/scalaint",
                                   classOf[ScalaCodeHandler], "getSessions", "Return all active session IDs", scalaCodeFactory)

    RequestServer.registerEndpoint("destroyScalaSession", "DELETE", "/3/scalaint/{session_id}",
                                   classOf[ScalaCodeHandler], "destroySession", "Return session id for communication with scala interpreter",
                                   scalaCodeFactory)
  }
}
