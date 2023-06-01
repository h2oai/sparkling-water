package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.backend.H2ORDD
import ai.h2o.sparkling.{H2OContext, H2OFrame}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * This converter just wraps the existing RDD converters and hides the internal RDD converters
  */
object SupportedRDDConverter {

  /** Transform supported type for conversion to a key string of H2OFrame */
  def toH2OFrame(hc: H2OContext, rdd: SupportedRDD, frameKeyName: Option[String]): H2OFrame = {
    rdd.toH2OFrame(hc, frameKeyName)
  }

  /** Transform H2OFrame to RDD */
  def toRDD[A <: Product: TypeTag: ClassTag](hc: H2OContext, fr: H2OFrame): RDD[A] = {
    new H2ORDD[A](fr)(hc)
  }
}
