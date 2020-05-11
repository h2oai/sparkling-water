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

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.backend.api.{GETRequestBase, POSTRequestBase, ServletRegister}
import ai.h2o.sparkling.utils.SparkSessionUtils
import javax.servlet.Servlet
import javax.servlet.http.HttpServletRequest
import org.apache.spark.h2o.H2OContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * This servlet class handles requests for /3/RDDs endpoint
  */
private[api] class RDDsInterpreterServlet() extends GETRequestBase with POSTRequestBase {
  private lazy val hc = H2OContext.ensure()
  private lazy val sc = hc.sparkContext

  private case class RDDInfoParameters(rddId: Int) {
    def validate(): Unit = {
      sc.getPersistentRDDs
        .getOrElse(rddId, throw new IllegalArgumentException(s"RDD with ID '$rddId' does not exist!"))
    }
  }

  private object RDDInfoParameters {
    def parse(request: HttpServletRequest): RDDInfoParameters = {
      val rddId = getParameterAsString(request, "rdd_id").toInt
      RDDInfoParameters(rddId)
    }
  }

  private case class RDDToH2OFrameParameters(rddId: Int, h2oFrameId: Option[String]) {
    def validate(): Unit = {
      sc.getPersistentRDDs
        .getOrElse(rddId, throw new IllegalArgumentException(s"RDD with ID '$rddId' does not exist!"))
    }
  }

  private object RDDToH2OFrameParameters {
    def parse(request: HttpServletRequest): RDDToH2OFrameParameters = {
      val rddId = getParameterAsString(request, "rdd_id").toInt
      val h2oFrameId = getParameterAsString(request, "h2o_frame_id")
      RDDToH2OFrameParameters(rddId, Option(h2oFrameId).map(_.toLowerCase()))
    }
  }

  override def handlePostRequest(request: HttpServletRequest): Any = {
    request.getServletPath match {
      case RDDsInterpreterServlet.convertRDDPath =>
        val parameters = RDDToH2OFrameParameters.parse(request)
        parameters.validate()
        toH2OFrame(parameters.rddId, parameters.h2oFrameId)
    }
  }

  override def handleGetRequest(request: HttpServletRequest): Any = {
    request.getServletPath match {
      case RDDsInterpreterServlet.getRDDsPath =>
        list()
      case RDDsInterpreterServlet.getRDDsPath =>
        val parameters = RDDInfoParameters.parse(request)
        parameters.validate()
        getRDD(parameters.rddId)
    }
  }

  def list(): RDDs = new RDDs(fetchAll())

  def getRDD(rddId: Int): RDDInfo = RDDInfo.fromRDD(sc.getPersistentRDDs(rddId))

  def toH2OFrame(rddId: Int, h2oFrameId: Option[String]): RDDToH2OFrame = {
    val rdd = sc.getPersistentRDDs(rddId)
    val h2oFrame = convertToH2OFrame(rdd, h2oFrameId)
    new RDDToH2OFrame(rddId, h2oFrame.frameId)
  }

  private def fetchAll(): Array[RDDInfo] = {
    SparkSessionUtils.active.sparkContext.getPersistentRDDs.values.map(RDDInfo.fromRDD).toArray
  }

  private def convertToH2OFrame(rdd: RDD[_], name: Option[String]): H2OFrame = {
    if (rdd.isEmpty()) {
      // transform empty Seq in order to create empty H2OFrame
      H2OFrame(hc.asH2OFrameKeyString(sc.parallelize(Seq.empty[Int]), name))
    } else {
      rdd.first() match {
        case _: Double => H2OFrame(hc.asH2OFrameKeyString(rdd.asInstanceOf[RDD[Double]], name))
        case _: LabeledPoint => H2OFrame(hc.asH2OFrameKeyString(rdd.asInstanceOf[RDD[LabeledPoint]], name))
        case _: Boolean => H2OFrame(hc.asH2OFrameKeyString(rdd.asInstanceOf[RDD[Boolean]], name))
        case _: String => H2OFrame(hc.asH2OFrameKeyString(rdd.asInstanceOf[RDD[String]], name))
        case _: Int => H2OFrame(hc.asH2OFrameKeyString(rdd.asInstanceOf[RDD[Int]], name))
        case _: Float => H2OFrame(hc.asH2OFrameKeyString(rdd.asInstanceOf[RDD[Float]], name))
        case _: Long => H2OFrame(hc.asH2OFrameKeyString(rdd.asInstanceOf[RDD[Long]], name))
        case _: java.sql.Timestamp =>
          H2OFrame(hc.asH2OFrameKeyString(rdd.asInstanceOf[RDD[java.sql.Timestamp]], name))
        case _: Product =>
          val first = rdd.asInstanceOf[RDD[Product]].first()
          val fields = ScalaReflection.getConstructorParameters(first.getClass).map { v =>
            val schema = ScalaReflection.schemaFor(v._2)
            StructField(v._1, schema.dataType, schema.nullable)
          }
          val df = SparkSessionUtils.active
            .createDataFrame(rdd.asInstanceOf[RDD[Product]].map(Row.fromTuple), StructType(fields))
          H2OFrame(hc.asH2OFrameKeyString(df, name))
        case t => throw new IllegalArgumentException(s"Do not understand type $t")
      }
    }
  }
}

object RDDsInterpreterServlet extends ServletRegister {

  private val getRDDsPath = "/3/RDDs"
  private val getRDDPath = "/3/RDD/*"
  private val convertRDDPath = "/3/RDD/*/h2oframe"
  override protected def getEndpoints(): Array[String] = {
    Array(getRDDsPath, getRDDPath, convertRDDPath)
  }

  override protected def getServletClass(): Class[_ <: Servlet] = classOf[RDDsInterpreterServlet]
}
