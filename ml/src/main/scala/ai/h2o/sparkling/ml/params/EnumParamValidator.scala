package ai.h2o.sparkling.ml.params

import java.util

object EnumParamValidator {

  def getValidatedEnumValue[T <: Enum[T]](name: String)(implicit ctag: reflect.ClassTag[T]): String = {
    getValidatedEnumValue(ctag.runtimeClass, name)
  }

  def getValidatedEnumValues[T <: Enum[T]](inputNames: Array[String], nullEnabled: Boolean = false)(
      implicit ctag: reflect.ClassTag[T]): Array[String] = {
    getValidatedEnumValues(ctag.runtimeClass, inputNames, nullEnabled)
  }

  // Method exposed for PySparkling so we can do the same checks there
  def getValidatedEnumValue(className: String, name: String): String = {
    getValidatedEnumValue(Class.forName(className), name)
  }

  // Method exposed for PySparkling so we can do the same checks there
  def getValidatedEnumValues(
      className: String,
      inputNames: util.ArrayList[String],
      nullEnabled: Boolean): Array[String] = {
    val arr = if (inputNames == null) null else inputNames.toArray(new Array[String](inputNames.size()))
    getValidatedEnumValues(Class.forName(className), arr, nullEnabled)
  }

  private def getValidatedEnumValue(clazz: Class[_], name: String): String = {
    val names = getEnumValues(clazz)
    if (name == null) {
      throw new IllegalArgumentException(s"Null is not a valid value. Allowed values are: ${names.mkString(", ")}")
    }

    if (!names.map(_.toLowerCase()).contains(name.toLowerCase())) {
      throw new IllegalArgumentException(s"'$name' is not a valid value. Allowed values are: ${names.mkString(", ")}")
    }
    names.find(_.toLowerCase() == name.toLowerCase).get
  }

  private def getValidatedEnumValues(
      clazz: Class[_],
      inputNames: Array[String],
      nullEnabled: Boolean): Array[String] = {
    val names = getEnumValues(clazz)
    if (inputNames == null) {
      if (nullEnabled) {
        return null
      } else {
        throw new IllegalArgumentException(
          s"Null is not a valid value. Allowed input is array with any of the following elements: ${names.mkString(", ")}")
      }
    }

    inputNames.foreach { name =>
      val nullStr = if (nullEnabled) " null or " else " "
      if (name == null) {
        throw new IllegalArgumentException(
          s"Null can not be specified as the input array element. " +
            s"Allowed input is${nullStr}array with any of the following elements: ${names.mkString(", ")}")
      }
      if (!names.map(_.toLowerCase()).contains(name.toLowerCase())) {
        throw new IllegalArgumentException(
          s"'$name' is not a valid value. Allowed input is${nullStr}array with" +
            s" any of the following elements: ${names.mkString(", ")}")
      }
    }

    names.filter(name => inputNames.map(_.toLowerCase).contains(name.toLowerCase()))
  }

  private def getEnumValues(clazz: Class[_]): Array[String] = {
    clazz.getDeclaredMethod("values").invoke(null).asInstanceOf[Array[Enum[_]]].map(_.name())
  }
}
