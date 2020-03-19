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

import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.SparkContext
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StructField, StructType}
import water.api.{Handler, HandlerFactory, RestApiContext}
import water.exceptions.H2ONotFoundArgumentException

/**
 * Handler for all RDD related queries
 */
class RDDsHandler(val sc: SparkContext, val h2oContext: H2OContext) extends Handler {

  def list(version: Int, s: RDDsV3): RDDsV3 = {
    val r = s.createAndFillImpl()
    r.rdds = fetchAll()
    s.fillFromImpl(r)
    s
  }

  def fetchAll(): Array[IcedRDD] =
    sc.getPersistentRDDs.values.map(IcedRDD.fromRdd).toArray

  def getRDD(version: Int, s: RDDV3): RDDV3 = {
    val rdd = sc.getPersistentRDDs.getOrElse(s.rdd_id,
      throw new H2ONotFoundArgumentException(s"RDD with ID '${s.rdd_id}' does not exist!"))

    s.name = Option(rdd.name).getOrElse(rdd.id.toString)
    s.partitions = rdd.partitions.length
    s
  }

  private[RDDsHandler] def convertToH2OFrame(rdd: RDD[_], name: Option[String]): H2OFrame = {
    if (rdd.isEmpty()) {
      // transform empty Seq in order to create empty H2OFrame
      h2oContext.asH2OFrame(sc.parallelize(Seq.empty[Int]), name)
    } else {
      rdd.first() match {
        case _: Double => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Double]], name)
        case _: LabeledPoint => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[LabeledPoint]], name)
        case _: Boolean => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Boolean]], name)
        case _: String => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[String]], name)
        case _: Int => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Int]], name)
        case _: Float => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Float]], name)
        case _: Long => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Long]], name)
        case _: java.sql.Timestamp => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[java.sql.Timestamp]], name)
        case _: Product =>
          val first = rdd.asInstanceOf[RDD[Product]].first()
          val fields = ScalaReflection.getConstructorParameters(first.getClass).map { v =>
            val schema = ScalaReflection.schemaFor(v._2)
            StructField(v._1, schema.dataType, schema.nullable)
          }
          val df = SparkSessionUtils.active.createDataFrame(rdd.asInstanceOf[RDD[Product]].map(Row.fromTuple), StructType(fields))
          h2oContext.asH2OFrame(df, name)
        case t => throw new IllegalArgumentException(s"Do not understand type $t")
      }
    }
  }

  def toH2OFrame(version: Int, s: RDD2H2OFrameIDV3): RDD2H2OFrameIDV3 = {
    val rdd = sc.getPersistentRDDs.getOrElse(s.rdd_id,
      throw new H2ONotFoundArgumentException(s"RDD with ID '${s.rdd_id}' does not exist, can not proceed with the transformation!"))

    val h2oFrame = convertToH2OFrame(rdd, Option(s.h2oframe_id) map (_.toLowerCase))
    s.h2oframe_id = h2oFrame._key.toString
    s
  }
}

object RDDsHandler {
  private[api] def registerEndpoints(context: RestApiContext, sc: SparkContext, h2oContext: H2OContext) = {

    val rddsHandler = new RDDsHandler(sc, h2oContext)

    def rddsFactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = rddsHandler
    }

    context.registerEndpoint("listRDDs", "GET", "/3/RDDs", classOf[RDDsHandler], "list",
      "Return all RDDs within Spark cloud", rddsFactory)

    context.registerEndpoint("getRDD", "POST", "/3/RDDs/{rdd_id}", classOf[RDDsHandler],
      "getRDD", "Get RDD with the given ID from Spark cloud", rddsFactory)

    context.registerEndpoint("rddToH2OFrame", "POST", "/3/RDDs/{rdd_id}/h2oframe",
      classOf[RDDsHandler], "toH2OFrame", "Transform RDD with the given ID to H2OFrame", rddsFactory)
  }
}
