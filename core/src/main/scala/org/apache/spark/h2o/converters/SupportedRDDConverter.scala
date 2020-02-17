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

package org.apache.spark.h2o.converters

import org.apache.spark._
import org.apache.spark.h2o._
import org.apache.spark.mllib.regression.LabeledPoint
import water.DKV
import water.fvec.{Frame, H2OFrame}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


/**
  * This converter just wraps the existing RDD converters and hides the internal RDD converters
  */

object SupportedRDDConverter {
  /** Transform supported type for conversion to a key string of H2OFrame */
  def toH2OFrameKeyString(
      hc: H2OContext,
      rdd: SupportedRDD,
      frameKeyName: Option[String],
      converter: WriteConverterCtxUtils.Converter): String = {
    rdd.toH2OFrameKeyString(hc, frameKeyName, converter)
  }

  /** Transform H2OFrame to RDD */
  def toRDD[A <: Product : TypeTag : ClassTag, T <: Frame](hc: H2OContext, fr: T): H2ORDD[A, T] = new H2ORDD[A, T](fr)(hc)

  /** Transform H2OFrame to RDD */
  def toRDD[A <: Product : TypeTag : ClassTag](hc: H2OContext, fr: ai.h2o.sparkling.frame.H2OFrame): H2ORESTRDD[A] = {
    new H2ORESTRDD[A](fr)(hc)
  }
}

/**
  * Magnet pattern (Type Class pattern) for conversion from various primitive types to their appropriate H2OFrame using
  * the method with the same name
  */
trait SupportedRDD {
  def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
    val converter = WriteConverterCtxUtils.getConverter(hc.getConf)
    val key = toH2OFrameKeyString(hc, frameKeyName, converter)
    new H2OFrame(DKV.getGet[Frame](key))
  }

  def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String
}

private[this] object SupportedRDD {

  implicit def toH2OFrameFromRDDJavaBool(rdd: RDD[java.lang.Boolean]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDJavaByte(rdd: RDD[java.lang.Byte]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDJavaShort(rdd: RDD[java.lang.Short]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDJavaInt(rdd: RDD[java.lang.Integer]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDJavaFloat(rdd: RDD[java.lang.Float]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDJavaDouble(rdd: RDD[java.lang.Double]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDJavaLong(rdd: RDD[java.lang.Long]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDBool(rdd: RDD[Boolean]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDByte(rdd: RDD[Byte]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDShort(rdd: RDD[Short]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDInt(rdd: RDD[Int]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDFloat(rdd: RDD[Float]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromDouble(rdd: RDD[Double]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDLong(rdd: RDD[Long]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDString(rdd: RDD[String]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDLabeledPoint(rdd: RDD[LabeledPoint]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDTimeStamp(rdd: RDD[java.sql.Timestamp]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.map(v => Tuple1(v)).toDF(), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDProductNoTypeTag(rdd: RDD[Product]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      SparkDataFrameConverter.toH2OFrameKeyString(hc, hc.sparkSession.createDataFrame(rdd), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDProduct[A <: Product : ClassTag : TypeTag](rdd: RDD[A]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      SparkDataFrameConverter.toH2OFrameKeyString(hc, hc.sparkSession.createDataFrame(rdd), frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDmlVector(rdd: RDD[ml.linalg.Vector]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.map(v => Tuple1(v)).toDF, frameKeyName, converter)
    }
  }

  implicit def toH2OFrameFromRDDMLlibVector(rdd: RDD[mllib.linalg.Vector]): SupportedRDD = new SupportedRDD {
    override def toH2OFrameKeyString(hc: H2OContext, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
      import hc.sparkSession.implicits._
      SparkDataFrameConverter.toH2OFrameKeyString(hc, rdd.map(v => Tuple1(v)).toDF, frameKeyName, converter)
    }
  }

}
