package org.apache.spark.h2o

import org.apache.spark.SparkContext
import water.fvec.Vec
import water.parser.Categorical

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Utilities to work with primitive types such as String, Integer, Double..
 *
 */
object H2OPrimitiveTypesUtils {

  /** Method used for obtaining column domains */
  private[spark]
  def collectColumnDomains[T](sc: SparkContext,
                              rdd: RDD[T],
                              fnames: Array[String],
                              ftypes: Array[Class[_]]): Array[Array[String]] = {
    val res = Array.ofDim[Array[String]](fnames.length)
    for (idx <- 0 until ftypes.length if ftypes(idx).equals(classOf[String])) {
      val acc = sc.accumulableCollection(new mutable.HashSet[String]())
      rdd.foreach(r => {
        acc += r.asInstanceOf[String]
      })
      res(idx) = if (acc.value.size > Categorical.MAX_ENUM_SIZE) null else acc.value.toArray.sorted
    }
    res
  }

  /** Method translating primitive types into Sparkling Water types
    * This method is already prepared to handle all mentioned primitive types  */
  private[spark]
  def dataTypeToVecType(t: Class[_], d: Array[String]): Byte = {
    t match {
      case q if q == classOf[java.lang.Byte] => Vec.T_NUM
      case q if q == classOf[java.lang.Short] => Vec.T_NUM
      case q if q == classOf[java.lang.Integer] => Vec.T_NUM
      case q if q == classOf[java.lang.Long] => Vec.T_NUM
      case q if q == classOf[java.lang.Float] => Vec.T_NUM
      case q if q == classOf[java.lang.Double] => Vec.T_NUM
      case q if q == classOf[java.lang.Boolean] => Vec.T_NUM
      case q if q == classOf[java.lang.String] => if (d != null && d.length < water.parser.Categorical.MAX_ENUM_SIZE) {
        Vec.T_ENUM
      } else {
        Vec.T_STR
      }
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }
}
