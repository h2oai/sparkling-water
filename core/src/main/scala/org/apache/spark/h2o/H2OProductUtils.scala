package org.apache.spark.h2o

import org.apache.spark.SparkContext
import water.fvec.Vec
import water.parser.Categorical

import scala.collection.mutable

/**
 * Utilities to work with Product classes.
 *
 */
object H2OProductUtils {

  private[spark]
  def dataTypeToVecType(t: Class[_]):Byte = {
    t match {
      case q if q==classOf[java.lang.Byte]    => Vec.T_NUM
      case q if q==classOf[java.lang.Short]   => Vec.T_NUM
      case q if q==classOf[java.lang.Integer] => Vec.T_NUM
      case q if q==classOf[java.lang.Long]    => Vec.T_NUM
      case q if q==classOf[java.lang.Float]   => Vec.T_NUM
      case q if q==classOf[java.lang.Double]  => Vec.T_NUM
      case q if q==classOf[java.lang.Boolean] => Vec.T_NUM
      case q if q==classOf[java.lang.String]  => Vec.T_STR
      case q if q==classOf[java.sql.Timestamp] => Vec.T_TIME
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }
}
