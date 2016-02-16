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
import org.apache.spark.h2o.{H2OFrame, H2OContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import water.Iced
import water.api.Handler

/**
 * RDDs handler.
 */
class RDDsHandler(val sc: SparkContext, val h2oContext: H2OContext) extends Handler {

  def list(version: Int, s: RDDsV3): RDDsV3 = {
    val r = s.createAndFillImpl()
    r.rdds = fetchAll()
    s.fillFromImpl(r)
    s
  }

  def fetchAll(): Array[IcedRDDInfo] =
    sc.getPersistentRDDs.values.map(IcedRDDInfo.fromRdd(_)).toArray

  def getRDD(version: Int, s: RDDWithMsgV3): RDDWithMsgV3 = {
    val r = s.createAndFillImpl()
    if (sc.getPersistentRDDs.get(s.searched_rdd_id).isEmpty) {
      s.msg = s"RDD with id '${s.searched_rdd_id}' does not exist"
    } else {
      r.rdd = IcedRDDInfo.fromRdd(sc.getPersistentRDDs.get(s.searched_rdd_id).get)
      s.fillFromImpl(r)
      s.msg = "OK"
    }
    s
  }

  private[RDDsHandler] def convertToH2OFrame(rdd: RDD[_]): H2OFrame = {
    if (rdd.isEmpty()) {
      // transform empty Seq in order to create empty H2OFrame
      h2oContext.asH2OFrame(sc.parallelize(Seq.empty[Int]))
    } else {
       rdd.first() match {
        case t if t.isInstanceOf[Double] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Double]])
        case t if t.isInstanceOf[LabeledPoint] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[LabeledPoint]])
        case t if t.isInstanceOf[Boolean] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Boolean]])
        case t if t.isInstanceOf[String] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[String]])
        case t if t.isInstanceOf[Int] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Int]])
        case t if t.isInstanceOf[Float] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Float]])
        case t if t.isInstanceOf[Long] => h2oContext.asH2OFrame(rdd.asInstanceOf[RDD[Long]])
        case t if t.isInstanceOf[Product] => H2OContext.toH2OFrameFromPureProduct(sc, rdd.asInstanceOf[RDD[Product]], None)
        case t => throw new IllegalArgumentException(s"Do not understand type $t")
      }
    }
  }

  def toH2OFrame(version: Int, s: RDD2H2OFrameIDV3): RDD2H2OFrameIDV3 = {
    if (sc.getPersistentRDDs.get(s.rdd_id).isEmpty) {
      s.h2oframe_id = ""
      s.msg = s"RDD with id '${s.rdd_id}' does not exist, can not proceed with the transformation"
    } else {
      val rdd = sc.getPersistentRDDs.get(s.rdd_id).get
      val h2oframe = convertToH2OFrame(rdd)
      s.h2oframe_id = h2oframe._key.toString
      s.msg = "Success"
    }
    s
  }
}

private[api] class IcedRDDInfo(val id: Int,
                               val name: String,
                               val partitions: Int) extends Iced[IcedRDDInfo] {

  def this() = this(-1, "", -1) // initialize with dummy values, this is used by the createImpl method in the
  //RequestServer as it calls constructor without any arguments
}

private[api] object IcedRDDInfo {
  def fromRdd(rdd: RDD[_]): IcedRDDInfo = {
    val rddName = Option(rdd.name).getOrElse(rdd.id.toString)
    new IcedRDDInfo(rdd.id, rddName, rdd.partitions.size)
  }
}

private[api] class IcedRDDWithMsgInfo() extends Iced[IcedRDDWithMsgInfo]{
  var rdd: IcedRDDInfo = _
}

/** Simple implementation pojo holding list of RDDs */
private[api] class RDDs extends Iced[RDDs] {
  var rdds: Array[IcedRDDInfo]  = _
}


private[api] class IcedRDD2H2OFrameID(val rdd_id: Integer) extends Iced[IcedRDD2H2OFrameID] {

  def this() = this(-1) // initialize with empty values, this is used by the createImpl method in the
  //RequestServer, as it calls constructor without any arguments
}

