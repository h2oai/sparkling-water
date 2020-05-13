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

import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest
import org.apache.spark.rdd.RDD

/** Schema representing /3/RDDs/[rdd_id] endpoint */
case class RDDInfo(rdd_id: Int, name: String, partitions: Int)

object RDDInfo extends ParameterBase with RDDCommons {
  def fromRDD(rdd: RDD[_]): RDDInfo = {
    new RDDInfo(rdd.id, Option(rdd.name).getOrElse(rdd.id.toString), rdd.partitions.length)
  }

  private[api] case class RDDInfoParameters(rddId: Int) {
    def validate(): Unit = validateRDDId(rddId)
  }

  private[api] object RDDInfoParameters {
    def parse(request: HttpServletRequest): RDDInfoParameters = {
      val rddId = request.getRequestURI.split("/")(3).toInt
      RDDInfoParameters(rddId)
    }
  }
}
