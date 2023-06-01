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
