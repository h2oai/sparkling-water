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

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import water.Key
import org.apache.spark._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Implicit transformations available on [[org.apache.spark.h2o.H2OContext]]
  */
abstract class H2OContextImplicits {

  protected def _h2oContext: H2OContext
  /** Implicit conversion from RDD[Supported type] to H2OFrame */
  implicit def asH2OFrameFromRDDProduct[A <: Product : ClassTag : TypeTag](rdd : RDD[A]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)
  implicit def asH2OFrameFromRDDString(rdd: RDD[String]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)
  implicit def asH2OFrameFromRDDBool(rdd: RDD[Boolean]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)
  implicit def asH2OFrameFromRDDDouble(rdd: RDD[Double]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)
  implicit def asH2OFrameFromRDDLong(rdd: RDD[Long]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)
  implicit def asH2OFrameFromRDDByte(rdd: RDD[Byte]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)
  implicit def asH2OFrameFromRDDShort(rdd: RDD[Short]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)
  implicit def asH2OFrameFromRDDTimeStamp(rdd: RDD[java.sql.Timestamp]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)
  implicit def asH2OFrameFromRDDLabeledPoint(rdd: RDD[LabeledPoint]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)
  implicit def asH2OFrameFromRDDMLlibVector(rdd: RDD[mllib.linalg.Vector]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)
  implicit def asH2OFrameFromRDDMlVector(rdd: RDD[ml.linalg.Vector]): H2OFrame = _h2oContext.asH2OFrame(rdd, None)


  /** Implicit conversion from RDD[Supported type] to H2OFrame key */
  implicit def toH2OFrameKeyFromRDDProduct[A <: Product : ClassTag : TypeTag](rdd : RDD[A]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)
  implicit def toH2OFrameKeyFromRDDString(rdd: RDD[String]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)
  implicit def toH2OFrameKeyFromRDDBool(rdd: RDD[Boolean]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)
  implicit def toH2OFrameKeyFromRDDDouble(rdd: RDD[Double]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)
  implicit def toH2OFrameKeyFromRDDLong(rdd: RDD[Long]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)
  implicit def toH2OFrameKeyFromRDDByte(rdd: RDD[Byte]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)
  implicit def toH2OFrameKeyFromRDDShort(rdd: RDD[Short]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)
  implicit def toH2OFrameKeyFromRDDTimeStamp(rdd: RDD[java.sql.Timestamp]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)
  implicit def toH2OFrameKeyFromRDDLabeledPoint(rdd: RDD[LabeledPoint]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)
  implicit def toH2OFrameKeyFromRDDMLlibVector(rdd: RDD[mllib.linalg.Vector]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)
  implicit def toH2OFrameKeyfromRDDMlVector(rdd: RDD[ml.linalg.Vector]): Key[_] = _h2oContext.toH2OFrameKey(rdd, None)



  /** Implicit conversion from Spark DataFrame to H2OFrame */
  implicit def asH2OFrameFromDataFrame(df : DataFrame) : H2OFrame = _h2oContext.asH2OFrame(df, None)

  /** Implicit conversion from Spark DataFrame to H2OFrame key */
  implicit def toH2OFrameKeyFromDataFrame(rdd : DataFrame) : Key[Frame] = _h2oContext.toH2OFrameKey(rdd, None)

  /** Implicit conversion from Spark Dataset to H2OFrame */
  implicit def asH2OFrameFromDataset[T<: Product : TypeTag](ds: Dataset[T]) : H2OFrame = _h2oContext.asH2OFrame(ds, None)

  /** Implicit conversion from Spark Dataset to H2OFrame key */
  implicit def toH2OFrameKeyFromDataset[T<: Product : TypeTag](ds: Dataset[T]) : Key[Frame] = _h2oContext.toH2OFrameKey(ds, None)

  /** Implicit conversion from Frame(H2O) to H2OFrame */
  implicit def asH2OFrameFromFrame(fr: Frame) : H2OFrame = new H2OFrame(fr)

  /** Implicit conversion from Frame(H2O) to H2OFrame key */
  implicit def toH2OFrameKeyFromFrame[T <: Frame](fr: T): Key[Frame] = fr._key

  implicit def toH2OFrameKeyFromH2OFrame(fr: H2OFrame): Key[Frame] = fr.key

  /** Transform given Scala symbol to String */
  implicit def symbolToString(sy: scala.Symbol): String = sy.name
}
