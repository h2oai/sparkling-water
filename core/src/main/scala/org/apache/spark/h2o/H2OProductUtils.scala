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
  def collectColumnDomains[A <: Product](sc: SparkContext,
                                         rdd: RDD[A],
                                         fnames: Array[String],
                                         ftypes: Array[Class[_]]): Array[Array[String]] = {
    val res = Array.ofDim[Array[String]](fnames.length)
    for (idx <- 0 until ftypes.length if ftypes(idx).equals(classOf[String])) {
      val acc =  sc.accumulableCollection(new mutable.HashSet[String]())
      // Distributed ops
      // FIXME product element can be Optional or Non-optional
      rdd.foreach( r => {
        val v = r.productElement(idx).asInstanceOf[Option[String]]
        if (v.isDefined) acc += v.get
      })
      res(idx) = if (acc.value.size > Categorical.MAX_ENUM_SIZE) null else acc.value.toArray.sorted
    }
    res
  }

  private[spark]
  def dataTypeToVecType(t: Class[_], d: Array[String]):Byte = {
    t match {
      case q if q==classOf[java.lang.Byte]    => Vec.T_NUM
      case q if q==classOf[java.lang.Short]   => Vec.T_NUM
      case q if q==classOf[java.lang.Integer] => Vec.T_NUM
      case q if q==classOf[java.lang.Long]    => Vec.T_NUM
      case q if q==classOf[java.lang.Float]   => Vec.T_NUM
      case q if q==classOf[java.lang.Double]  => Vec.T_NUM
      case q if q==classOf[java.lang.Boolean] => Vec.T_NUM
      case q if q==classOf[java.lang.String]  => if (d!=null && d.length < water.parser.Categorical.MAX_ENUM_SIZE) {
                                                  Vec.T_ENUM
                                                } else {
                                                  Vec.T_STR
                                                }
      case q if q==classOf[java.sql.Timestamp] => Vec.T_TIME
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }
}
