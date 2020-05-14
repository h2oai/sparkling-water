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

package ai.h2o.sparkling.backend

import ai.h2o.sparkling.{H2OContext, H2OFrame}
import org.apache.spark._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Implicit transformations available on [[ai.h2o.sparkling.H2OContext]]
  */
abstract class H2OContextImplicits {

  protected def hc: H2OContext

  implicit def asH2OFrameFromRDDProduct[A <: Product: ClassTag: TypeTag](rdd: RDD[A]): H2OFrame =
    hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromRDDString(rdd: RDD[String]): H2OFrame = hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromRDDBool(rdd: RDD[Boolean]): H2OFrame = hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromRDDDouble(rdd: RDD[Double]): H2OFrame = hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromRDDLong(rdd: RDD[Long]): H2OFrame = hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromRDDByte(rdd: RDD[Byte]): H2OFrame = hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromRDDShort(rdd: RDD[Short]): H2OFrame = hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromRDDTimeStamp(rdd: RDD[java.sql.Timestamp]): H2OFrame = hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromRDDLabeledPoint(rdd: RDD[LabeledPoint]): H2OFrame = hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromRDDMLlibVector(rdd: RDD[mllib.linalg.Vector]): H2OFrame = hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromRDDMlVector(rdd: RDD[ml.linalg.Vector]): H2OFrame = hc.asH2OFrame(rdd, None)

  implicit def asH2OFrameFromDataFrame(df: DataFrame): H2OFrame = hc.asH2OFrame(df, None)

  implicit def asH2OFrameFromDataset[T <: Product: ClassTag: TypeTag](ds: Dataset[T]): H2OFrame =
    hc.asH2OFrame(ds, None)
}
