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

package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.{H2OContext, H2OFrame}
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


/**
 * Magnet pattern (Type Class pattern) for conversion from various primitive types to their appropriate H2OFrame using
 * the method with the same name
 */
trait SupportedRDD {
  def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame
}

private[this] object SupportedRDD {

  implicit def toH2OFrameFromRDDJavaBool(rdd: RDD[java.lang.Boolean]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDJavaByte(rdd: RDD[java.lang.Byte]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDJavaShort(rdd: RDD[java.lang.Short]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDJavaInt(rdd: RDD[java.lang.Integer]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDJavaFloat(rdd: RDD[java.lang.Float]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDJavaDouble(rdd: RDD[java.lang.Double]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDJavaLong(rdd: RDD[java.lang.Long]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDBool(rdd: RDD[Boolean]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDByte(rdd: RDD[Byte]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDShort(rdd: RDD[Short]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDInt(rdd: RDD[Int]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDFloat(rdd: RDD[Float]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromDouble(rdd: RDD[Double]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDLong(rdd: RDD[Long]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDString(rdd: RDD[String]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDLabeledPoint(rdd: RDD[LabeledPoint]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDTimeStamp(rdd: RDD[java.sql.Timestamp]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.map(v => Tuple1(v)).toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDProductNoTypeTag(rdd: RDD[Product]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val df = SparkSessionUtils.active.createDataFrame(rdd)
      SparkDataFrameConverter.toH2OFrame(hc, df, frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDProduct[A <: Product : ClassTag : TypeTag](rdd: RDD[A]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val df = SparkSessionUtils.active.createDataFrame(rdd)
      SparkDataFrameConverter.toH2OFrame(hc, df, frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDmlVector(rdd: RDD[ml.linalg.Vector]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.map(v => Tuple1(v)).toDF, frameKeyName)
    }
  }

  implicit def toH2OFrameFromRDDMLlibVector(rdd: RDD[mllib.linalg.Vector]): SupportedRDD = new SupportedRDD {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      SparkDataFrameConverter.toH2OFrame(hc, rdd.map(v => Tuple1(v)).toDF, frameKeyName)
    }
  }
}
