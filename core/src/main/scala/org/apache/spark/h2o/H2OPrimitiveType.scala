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
package org.apache.spark.h2o

import org.apache.spark.SparkContext

/**
 * Magnet pattern (Type Class pattern) for conversion from various primitive types to their appropriate H2OFrame using
 * the method with the same name
 */
trait PrimitiveType {
  def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame
}


object PrimitiveType {
  implicit def toDataFrameFromString(rdd: RDD[String]): PrimitiveType = new PrimitiveType {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDString(sc, rdd, frameKeyName)
  }

  implicit def toDataFrameFromInt(rdd: RDD[Int]): PrimitiveType = new PrimitiveType {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDInt(sc, rdd, frameKeyName)
  }

  implicit def toDataFrameFromFloat(rdd: RDD[Float]): PrimitiveType = new PrimitiveType {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDFloat(sc, rdd, frameKeyName)
  }

  implicit def toDataFrameFromDouble(rdd: RDD[Double]): PrimitiveType = new PrimitiveType {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDDouble(sc, rdd, frameKeyName)
  }

  implicit def toDataFrameFromBool(rdd: RDD[Boolean]): PrimitiveType = new PrimitiveType {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDBool(sc, rdd, frameKeyName)
  }

  implicit def toDataFrameFromLong(rdd: RDD[Long]): PrimitiveType = new PrimitiveType {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDLong(sc, rdd, frameKeyName)
  }
}
