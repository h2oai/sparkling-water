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
import org.apache.spark.mllib.regression.LabeledPoint

import scala.language.implicitConversions
import scala.reflect.runtime.universe._


/**
  * Magnet pattern (Type Class pattern) for conversion from various primitive types to their appropriate H2OFrame using
  * the method with the same name
  */
trait SupportedRDD {
  def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame
}

object SupportedRDD {

  implicit def toH2OFrameFromRDDString(rdd: RDD[String]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDString(sc, rdd, frameKeyName)
  }

  implicit def toH2OFrameFromRDDInt(rdd: RDD[Int]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDInt(sc, rdd, frameKeyName)
  }

  implicit def toH2OFrameFromRDDByte(rdd: RDD[Byte]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDByte(sc, rdd, frameKeyName)
  }

  implicit def toH2OFrameFromRDDShort(rdd: RDD[Short]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDShort(sc, rdd, frameKeyName)
  }

  implicit def toH2OFrameFromRDDFloat(rdd: RDD[Float]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDFloat(sc, rdd, frameKeyName)
  }

  implicit def toH2OFrameFromDouble(rdd: RDD[Double]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDDouble(sc, rdd, frameKeyName)
  }

  implicit def toH2OFrameFromRDDBool(rdd: RDD[Boolean]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDBool(sc, rdd, frameKeyName)
  }

  implicit def toH2OFrameFromRDDLong(rdd: RDD[Long]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDLong(sc, rdd, frameKeyName)
  }

  implicit def toH2OFrameFromRDDLabeledPoint(rdd: RDD[LabeledPoint]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDLabeledPoint(sc, rdd, frameKeyName)
  }

  implicit def toH2OFrameFromRDDTimeStamo(rdd: RDD[java.sql.Timestamp]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromRDDTimeStamp(sc, rdd, frameKeyName)
  }

  implicit def toH2OFrameFromRDDProduct[A <: Product : TypeTag](rdd : RDD[A]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(sc: SparkContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrame(sc, rdd, frameKeyName)
  }
}
