package org.apache.spark.h2o


import org.apache.spark.sql.SQLContext

import scala.language.implicitConversions

  /**
  * Magnet pattern (Type Class pattern) for conversion from various case classes to their appropriate H2OFrame using
  * the method with the same name
  * Created by vpatryshev on 7/15/16.
  */
trait SupportedDataset {
  def toH2OFrame(sc: SQLContext, frameKeyName: Option[String]): H2OFrame
}

object SupportedDataset {

  implicit def toH2OFrameFromDataset[T <: Product](ds: Dataset[T]): SupportedDataset = new SupportedDataset {
    override def toH2OFrame(sc: SQLContext, frameKeyName: Option[String]): H2OFrame = H2OContext.toH2OFrameFromDataset(sc.sparkContext, ds, frameKeyName)
  }
}
