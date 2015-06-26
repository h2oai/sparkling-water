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
import org.apache.spark.rdd.RDD
import water.Iced
import water.api.Handler

/**
 * RDDs handler.
 */
class RDDsHandler(val sc: SparkContext) extends Handler {

  def list(version:Int, s: RDDsV3): RDDsV3 = {
    val r = s.createAndFillImpl()
    r.rdds = fetchAll()
    s.fillFromImpl(r)
    s
  }

  def fetchAll():Array[IcedRDDInfo] =
    sc.getPersistentRDDs.values.map(IcedRDDInfo.fromRdd(_)).toArray

  def getRDD(version: Int, s: RDDV3): RDDV3 = {
    var rdd = s.createAndFillImpl()
    if (sc.getPersistentRDDs.get(s.rdd_id).isEmpty) {
      s.rdd_id = -1
    } else {
      rdd = IcedRDDInfo.fromRdd(sc.getPersistentRDDs.get(s.rdd_id).get)
      s.fillFromImpl(rdd)
    }
    s
  }
}

/** Simple implementation pojo holding list of RDDs */
private[api] class IcedRDDs extends Iced[IcedRDDs] {
  var rdds: Array[IcedRDDInfo]  = _
}

private[api] class IcedRDDInfo(val rdd_id: Int,
                               val name: String,
                               val partitions: Int) extends Iced[IcedRDDInfo] {

  def this() = this(-1, "-1", -1) // initialize with dummy values, this is used by the createImpl method in the
  //RequestServer as it calls constructor without any arguments
}


private[api] object IcedRDDInfo {
  def fromRdd(rdd: RDD[_]): IcedRDDInfo = {
    val rddName = Option(rdd.name).getOrElse(rdd.id.toString)
    new IcedRDDInfo(rdd.id, rddName, rdd.partitions.size)
  }
}
