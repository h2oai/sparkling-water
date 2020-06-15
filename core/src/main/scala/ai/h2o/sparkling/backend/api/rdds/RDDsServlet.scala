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

package ai.h2o.sparkling.backend.api.rdds

import ai.h2o.sparkling.{H2OConf, H2OContext, H2OFrame}
import ai.h2o.sparkling.backend.api.{ServletBase, ServletRegister}
import ai.h2o.sparkling.utils.SparkSessionUtils
import javax.servlet.Servlet
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StructField, StructType}
import water.exceptions.H2ONotFoundArgumentException

/**
  * This servlet class handles requests for /3/RDDs endpoint
  */
private[api] class RDDsServlet extends ServletBase {
  private lazy val hc = H2OContext.ensure()
  private lazy val sc = hc.sparkContext

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case null => list()
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)
  }

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case s if s.matches("/.*/h2oframe") =>
        val parameters = RDDToH2OFrame.RDDToH2OFrameParameters.parse(req)
        parameters.validate()
        toH2OFrame(parameters.rddId, parameters.h2oFrameId)
      case s if s.matches("/.*") =>
        val parameters = RDDInfo.RDDInfoParameters.parse(req)
        parameters.validate()
        getRDD(parameters.rddId)
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)
  }

  def list(): RDDs = RDDs(fetchAll())

  def getRDD(rddId: Int): RDDInfo = RDDInfo.fromRDD(sc.getPersistentRDDs(rddId))

  def toH2OFrame(rddId: Int, h2oFrameId: Option[String]): RDDToH2OFrame = {
    val rdd = sc.getPersistentRDDs(rddId)
    val h2oFrame = convertToH2OFrame(rdd, h2oFrameId)
    RDDToH2OFrame(rddId, h2oFrame.frameId)
  }

  private def fetchAll(): Array[RDDInfo] = {
    SparkSessionUtils.active.sparkContext.getPersistentRDDs.values.map(RDDInfo.fromRDD).toArray
  }

  private def convertToH2OFrame(rdd: RDD[_], name: Option[String]): H2OFrame = {
    if (rdd.isEmpty()) {
      // transform empty Seq in order to create empty H2OFrame
      hc.asH2OFrame(sc.parallelize(Seq.empty[Int]), name)
    } else {
      rdd.first() match {
        case _: Double => hc.asH2OFrame(rdd.asInstanceOf[RDD[Double]], name)
        case _: LabeledPoint => hc.asH2OFrame(rdd.asInstanceOf[RDD[LabeledPoint]], name)
        case _: Boolean => hc.asH2OFrame(rdd.asInstanceOf[RDD[Boolean]], name)
        case _: String => hc.asH2OFrame(rdd.asInstanceOf[RDD[String]], name)
        case _: Int => hc.asH2OFrame(rdd.asInstanceOf[RDD[Int]], name)
        case _: Float => hc.asH2OFrame(rdd.asInstanceOf[RDD[Float]], name)
        case _: Long => hc.asH2OFrame(rdd.asInstanceOf[RDD[Long]], name)
        case _: java.sql.Timestamp =>
          hc.asH2OFrame(rdd.asInstanceOf[RDD[java.sql.Timestamp]], name)
        case _: Product =>
          val first = rdd.asInstanceOf[RDD[Product]].first()
          val fields = ScalaReflection.getConstructorParameters(first.getClass).map { v =>
            val schema = ScalaReflection.schemaFor(v._2)
            StructField(v._1, schema.dataType, schema.nullable)
          }
          val df = SparkSessionUtils.active
            .createDataFrame(rdd.asInstanceOf[RDD[Product]].map(Row.fromTuple), StructType(fields))
          hc.asH2OFrame(df, name)
        case t => throw new IllegalArgumentException(s"Do not understand type $t")
      }
    }
  }
}

object RDDsServlet extends ServletRegister {

  override protected def getRequestPaths(): Array[String] = {
    Array("/3/RDDs", "/3/RDDs/*")
  }

  override protected def getServlet(conf: H2OConf): Servlet = new RDDsServlet
}
