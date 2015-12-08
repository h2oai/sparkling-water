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
}

private[api] class IcedRDDInfo(val rdd_id: Int,
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



