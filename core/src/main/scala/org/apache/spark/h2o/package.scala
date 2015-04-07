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

package org.apache.spark

/** Type shortcuts to simplify work in Sparkling REPL */
package object h2o {
  type Frame = water.fvec.Frame
  //type Key = water.Key
  type H2O = water.H2O

  /* Cannot be enabled since clashes with Spark DataFrame
  @deprecated("1.3.0", "Use H2OFrame")
  type DataFrame = water.fvec.H2OFrame
  */
  // Alias for H2OFrame
  type H2OFrame = water.fvec.H2OFrame
  // Alias for
  type RDD[X] = org.apache.spark.rdd.RDD[X]

  case class IntHolder   (result: Option[Int])
  case class DoubleHolder(result: Option[Double])
  case class StringHolder(result: Option[String])
}
