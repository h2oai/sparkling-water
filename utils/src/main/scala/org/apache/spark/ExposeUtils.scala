package org.apache.spark

import org.apache.spark.sql.types.{DataType, UserDefinedType}

object ExposeUtils {
  def classForName(className: String): Class[_] = {
    org.apache.spark.util.Utils.classForName(className)
  }

  def isMLVectorUDT(dataType: DataType): Boolean = {
    dataType match {
      case _ : ml.linalg.VectorUDT => true
      case _ => false
    }
  }

  def isAnyVectorUDT(dataType: DataType): Boolean = {
    dataType match {
      case _ : ml.linalg.VectorUDT => true
      case _ : mllib.linalg.VectorUDT => true
      case _ => false
    }
  }

  def isUDT(dataType: DataType): Boolean = {
    dataType match {
      case _ : UserDefinedType[_] => true
      case _ => false
    }
  }
}
