package org.apache.spark.h2o

import org.apache.spark.SparkContext

/**
 * Magnet pattern (Type Class pattern) for conversion from various primitive types to their appropriate H2OFrame using
 * the method with the same name
 */
trait PrimitiveType {
  def toH2OFrame(sc: SparkContext): H2OFrame
}


object PrimitiveType {
  implicit def toDataFrameFromString(rdd: RDD[String]): PrimitiveType = new PrimitiveType {
    override def toH2OFrame(sc: SparkContext): H2OFrame = H2OContext.toH2OFrameFromRDDString(sc, rdd)
  }

  implicit def toDataFrameFromInt(rdd: RDD[Int]): PrimitiveType = new PrimitiveType {
    override def toH2OFrame(sc: SparkContext): H2OFrame = H2OContext.toH2OFrameFromRDDInt(sc, rdd)
  }

  implicit def toDataFrameFromFloat(rdd: RDD[Float]): PrimitiveType = new PrimitiveType {
    override def toH2OFrame(sc: SparkContext): H2OFrame = H2OContext.toH2OFrameFromRDDFloat(sc, rdd)
  }

  implicit def toDataFrameFromDouble(rdd: RDD[Double]): PrimitiveType = new PrimitiveType {
    override def toH2OFrame(sc: SparkContext): H2OFrame = H2OContext.toH2OFrameFromRDDDouble(sc, rdd)
  }
}
