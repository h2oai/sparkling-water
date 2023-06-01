package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.{H2OContext, H2OFrame}
import org.apache.spark._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Dataset

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Magnet pattern (Type Class pattern) for conversion from various primitive types to their appropriate H2OFrame using
  * the method with the same name
  */
trait SupportedDataset {
  def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame
}

private[this] object SupportedDataset {

  implicit def toH2OFrameFromDatasetJavaBool(dataset: Dataset[java.lang.Boolean]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetJavaByte(dataset: Dataset[java.lang.Byte]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetJavaShort(dataset: Dataset[java.lang.Short]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetJavaInt(dataset: Dataset[java.lang.Integer]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetJavaFloat(dataset: Dataset[java.lang.Float]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetJavaDouble(dataset: Dataset[java.lang.Double]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetJavaLong(dataset: Dataset[java.lang.Long]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetBool(dataset: Dataset[Boolean]): SupportedDataset = new SupportedDataset {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromDatasetByte(dataset: Dataset[Byte]): SupportedDataset = new SupportedDataset {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromDatasetShort(dataset: Dataset[Short]): SupportedDataset = new SupportedDataset {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromDatasetInt(dataset: Dataset[Int]): SupportedDataset = new SupportedDataset {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromDatasetFloat(dataset: Dataset[Float]): SupportedDataset = new SupportedDataset {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromDouble(dataset: Dataset[Double]): SupportedDataset = new SupportedDataset {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromDatasetLong(dataset: Dataset[Long]): SupportedDataset = new SupportedDataset {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromDatasetString(dataset: Dataset[String]): SupportedDataset = new SupportedDataset {
    override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
      SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
    }
  }

  implicit def toH2OFrameFromDatasetLabeledPoint(dataset: Dataset[LabeledPoint]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetTimeStamp(dataset: Dataset[java.sql.Timestamp]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        val spark = SparkSessionUtils.active
        import spark.implicits._
        SparkDataFrameConverter.toH2OFrame(hc, dataset.map(v => Tuple1(v)).toDF(), frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetProduct[A <: Product: ClassTag: TypeTag](dataset: Dataset[A]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        SparkDataFrameConverter.toH2OFrame(hc, dataset.toDF(), frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetmlVector(dataset: Dataset[ml.linalg.Vector]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        val spark = SparkSessionUtils.active
        import spark.implicits._
        SparkDataFrameConverter.toH2OFrame(hc, dataset.map(v => Tuple1(v)).toDF, frameKeyName)
      }
    }

  implicit def toH2OFrameFromDatasetMLlibVector(dataset: Dataset[mllib.linalg.Vector]): SupportedDataset =
    new SupportedDataset {
      override def toH2OFrame(hc: H2OContext, frameKeyName: Option[String]): H2OFrame = {
        val spark = SparkSessionUtils.active
        import spark.implicits._
        SparkDataFrameConverter.toH2OFrame(hc, dataset.map(v => Tuple1(v)).toDF, frameKeyName)
      }
    }
}
