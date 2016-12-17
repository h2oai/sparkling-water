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
package water.api.RDDs

import org.apache.spark.SparkContext
import org.apache.spark.h2o.converters.H2OFrameFromRDDProductBuilder
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import water.Iced
import water.api.Handler
import water.exceptions.H2ONotFoundArgumentException

/**
  * Handler for all RDD related queries
  */
class RDDsHandler(val sc: SparkContext, val h2oContext: H2OContext) extends Handler{

  def list(version: Int, s: RDDsV3): RDDsV3 = {
    val r = s.createAndFillImpl()
    r.rdds = fetchAll()
    s.fillFromImpl(r)
    s
  }

  def fetchAll(): Array[IcedRDDInfo] =
    sc.getPersistentRDDs.values.map(IcedRDDInfo.fromRdd).toArray

  def getRDD(version: Int, s: RDDV3): RDDV3 = {
    val rdd = sc.getPersistentRDDs.getOrElse(s.rdd_id,
      throw new H2ONotFoundArgumentException(s"RDD with ID '${s.rdd_id}' does not exist!"))

    s.name = Option(rdd.name).getOrElse(rdd.id.toString)
    s.partitions = rdd.partitions.length
    s
  }

  // TODO(vlad): fix this 'instanceOf'
  private[RDDsHandler] def convertToH2OFrame(rdd: RDD[_], name: Option[String]): H2OFrame = {
    if (rdd.isEmpty()) {
      // transform empty Seq in order to create empty H2OFrame
      h2oContext.asH2OFrame(sc.parallelize(Seq.empty[Int]),name)
    } else {
      rdd.first() match {
        case t if t.isInstanceOf[Double] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Double]], name)
        case t if t.isInstanceOf[LabeledPoint] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[LabeledPoint]], name)
        case t if t.isInstanceOf[Boolean] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Boolean]], name)
        case t if t.isInstanceOf[String] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[String]], name)
        case t if t.isInstanceOf[Int] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Int]], name)
        case t if t.isInstanceOf[Float] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Float]], name)
        case t if t.isInstanceOf[Long] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Long]], name)
        case t if t.isInstanceOf[java.sql.Timestamp] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[java.sql.Timestamp]], name)
        case t if t.isInstanceOf[Product] => H2OFrameFromRDDProductBuilder(h2oContext, rdd.asInstanceOf[RDD[Product]], name).withDefaultFieldNames()
        case t => throw new IllegalArgumentException(s"Do not understand type $t")
      }
    }
  }

  // TODO(vlad): see the same code in DataFrames
  def toH2OFrame(version: Int, s: RDD2H2OFrameIDV3): RDD2H2OFrameIDV3 = {
    val rdd = sc.getPersistentRDDs.getOrElse(s.rdd_id,
      throw new H2ONotFoundArgumentException(s"RDD with ID '${s.rdd_id}' does not exist, can not proceed with the transformation!"))

    // TODO(vlad): take care of the cases when the data are missing
    val h2oFrame = convertToH2OFrame(rdd, Option(s.h2oframe_id) map (_.toLowerCase))
    s.h2oframe_id = h2oFrame._key.toString
    s
  }
}

private[api] class IcedRDDInfo(val rdd_id: Int,
                               val name: String,
                               val partitions: Int) extends Iced[IcedRDDInfo] {

  def this() = this(-1, null, -1) // initialize with dummy values, this is used by the createImpl method in the
  //RequestServer as it calls constructor without any arguments
}

private[api] object IcedRDDInfo {
  def fromRdd(rdd: RDD[_]): IcedRDDInfo = {
    val rddName = Option(rdd.name).getOrElse(rdd.id.toString)
    new IcedRDDInfo(rdd.id, rddName, rdd.partitions.length)
  }
}

/** Simple implementation pojo holding list of RDDs */
private[api] class RDDs extends Iced[RDDs] {
  var rdds: Array[IcedRDDInfo]  = _
}


private[api] class IcedRDD2H2OFrameID(val rdd_id: Integer, val h2oframe_id: String) extends Iced[IcedRDD2H2OFrameID] {

  def this() = this(-1, null) // initialize with empty values, this is used by the createImpl method in the
  //RequestServer, as it calls constructor without any arguments
}

