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
package ai.h2o.sparkling.backend.internal

import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext}

import scala.reflect.ClassTag

private[internal] abstract class H2OAwareEmptyRDD[U: ClassTag](sc: SparkContext, nodes: Seq[NodeDesc]) extends RDD[U](sc, Nil) {

  override def getPreferredLocations(split: Partition): Seq[String] = {
    nodes.map(nodeDesc => s"executor_${nodeDesc.hostname}_${nodeDesc.nodeId}")
  }
}
