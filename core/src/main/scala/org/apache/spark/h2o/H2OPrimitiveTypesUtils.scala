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

  /** Method translating primitive types into Sparkling Water types
    * This method is already prepared to handle all mentioned primitive types  */
  private[spark]
  def dataTypeToVecType(t: Class[_]): Byte = {
    t match {
      case q if q == classOf[java.lang.Byte] => Vec.T_NUM
      case q if q == classOf[java.lang.Short] => Vec.T_NUM
      case q if q == classOf[java.lang.Integer] => Vec.T_NUM
      case q if q == classOf[java.lang.Long] => Vec.T_NUM
      case q if q == classOf[java.lang.Float] => Vec.T_NUM
      case q if q == classOf[java.lang.Double] => Vec.T_NUM
      case q if q == classOf[java.lang.Boolean] => Vec.T_NUM
      case q if q == classOf[java.lang.String] => Vec.T_STR
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }
}
