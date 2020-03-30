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

import org.apache.spark.rdd.RDD
import water.Iced

private[rdds] class IcedRDD(val rdd_id: Int, val name: String, val partitions: Int) extends Iced[IcedRDD] {

  def this() = this(-1, null, -1) // initialize with dummy values, this is used by the createImpl method in the
  //RequestServer as it calls constructor without any arguments
}

private[rdds] object IcedRDD {
  def fromRdd(rdd: RDD[_]): IcedRDD = {
    val rddName = Option(rdd.name).getOrElse(rdd.id.toString)
    new IcedRDD(rdd.id, rddName, rdd.partitions.length)
  }
}
