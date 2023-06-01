package ai.h2o.sparkling.doc.generation

import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.backend.external.ExternalBackendConf
import ai.h2o.sparkling.backend.internal.InternalBackendConf
import scala.reflect.runtime.universe._

import scala.collection.mutable.ArrayBuffer

object ConfigurationsTemplate {

  private val headerName = "Property name"
  private val headerValue = "Default value"
  private val headerSetter = "H2OConf setter (* getter_)"
  private val headerDescription = "Description"

  def apply(): String = {
    val sharedConfOptions = getOptions[SharedBackendConf.type](SharedBackendConf)
    val internalConfOptions = getOptions[InternalBackendConf.type](InternalBackendConf)
    val externalConfOptions = getOptions[ExternalBackendConf.type](ExternalBackendConf)

    s""".. _sw_config_properties:
       |
       |Sparkling Water Configuration Properties
       |----------------------------------------
       |
       |The following configuration properties can be passed to Spark to configure Sparking Water.
       |
       |Configuration properties independent of selected backend
       |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
       |
       |${generateTable(sharedConfOptions)}
       |--------------
       |
       |Internal backend configuration properties
       |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
       |
       |${generateTable(internalConfOptions)}
       |--------------
       |
       |External backend configuration properties
       |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
       |
       |${generateTable(externalConfOptions)}
       |--------------
       |
       |.. _getter:
       |
       |H2OConf getter can be derived from the corresponding setter. All getters are parameter-less. If the type of the property is Boolean, the getter is prefixed with
       |``is`` (E.g. ``setReplEnabled()`` -> ``isReplEnabled()``). Property getters of other types do not have any prefix and start with lowercase
       |(E.g. ``setUserName(String)`` -> ``userName`` for Scala, ``userName()`` for Python).
     """.stripMargin
  }

  case class Option(name: String, value: String, setters: String, doc: String)

  private def getOptions[T](t: Object)(implicit tag: TypeTag[T]): Array[Option] = {
    val ru = scala.reflect.runtime.universe
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val instanceMirror = rm.reflect(t)
    val typ = ru.typeOf[T]
    val members = typ.members.filter(_.isPublic).filter(_.name.toString.startsWith("PROP_"))
    val reflectedMembers = members.map(_.asTerm).map(instanceMirror.reflectField)
    reflectedMembers
      .map { member =>
        val optionTuple = member.get.asInstanceOf[(String, Any, String, String)]
        Option(optionTuple._1, optionTuple._2.toString, optionTuple._3, optionTuple._4)
      }
      .toArray
      .reverse
  }

  case class LineSizes(nameLength: Int, valueLength: Int, setterLength: Int, docLength: Int)

  private def generateTable(options: Array[Option]): String = {
    val sizes = computeMaxSizes(options)
    val builder = new StringBuilder
    builder
      .append(lineSeparator(sizes, "-"))
      .append("\n")
      .append(fillHeader(sizes))
      .append("\n")
      .append(lineSeparator(sizes, "="))
      .append("\n")

    options.foreach { option =>
      builder
        .append(fillOption(sizes, option.name, option.value, option.setters, option.doc))
        .append("\n")
        .append(lineSeparator(sizes, "-"))
        .append("\n")
    }
    builder.toString()
  }

  private def computeMaxSizes(options: Array[Option]): LineSizes = {
    val nameMaxLength = options.map(_.name.length).max
    val valueMaxLength = options.map(_.value.length).max
    val setterMaxLength = options.map(_.setters.split("\n").map(_.length).max).max
    val descriptionMaxLength = options.map(_.doc.split("\n").map(_.length).max).max

    val maxNameLength = if (headerName.length > nameMaxLength) headerName.length else nameMaxLength
    val maxValueLength = if (headerValue.length > valueMaxLength) headerValue.length else valueMaxLength
    val maxSetterLength = if (headerSetter.length > setterMaxLength) headerSetter.length else setterMaxLength
    val maxDocLength =
      if (headerDescription.length > descriptionMaxLength) headerDescription.length
      else descriptionMaxLength
    LineSizes(maxNameLength, maxValueLength, maxSetterLength, maxDocLength)
  }

  private def fillHeader(sizes: LineSizes): String = {
    val builder = new StringBuilder
    builder
      .append("|")
      .append("| ")
      .append(headerName)
      .append(repeat(sizes.nameLength - headerName.length + 4, " "))
      .append(" | ")
      .append(headerValue)
      .append(repeat(sizes.valueLength - headerValue.length, " "))
      .append(" | ")
      .append(headerSetter)
      .append(repeat(sizes.setterLength - headerSetter.length + 4, " "))
      .append(" | ")
      .append(headerDescription)
      .append(repeat(sizes.docLength - headerDescription.length, " "))
      .append(" |")
    builder.toString()
  }

  private def fillOption(sizes: LineSizes, name: String, value: String, setter: String, doc: String): String = {
    val builder = new StringBuilder

    val setterLinesRaw = setter.split("\n")

    val arrayBuffer = new ArrayBuffer[String]()
    arrayBuffer += setterLinesRaw.head
    setter.split("\n").tail.foreach { line =>
      arrayBuffer += ""
      arrayBuffer += line
    }
    val setterLines = arrayBuffer.toArray
    val docLines = doc.split("\n")
    val setterLinesLen = setterLines.length
    val docLinesLen = docLines.length
    val maxBoxRows = if (setterLinesLen >= docLinesLen) setterLinesLen else docLinesLen

    // First Row
    builder
      .append("|")
      .append("| ")
      .append("``")
      .append(name)
      .append("``")
      .append(repeat(sizes.nameLength - name.length, " "))
      .append(" | ")
      .append(value)
      .append(repeat(sizes.valueLength - value.length, " "))
      .append(" | ")
      .append("``")
      .append(setterLines.head)
      .append("``")
      .append(repeat(sizes.setterLength - setterLines.head.length, " "))
      .append(" | ")
      .append(docLines.head)
      .append(repeat(sizes.docLength - docLines.head.length, " "))
      .append(" |")

    // Additional Rows
    (1 until maxBoxRows).foreach { rowNum =>
      val nextSetterLine =
        if (rowNum >= setterLinesLen) ""
        else {
          if (setterLines(rowNum) == "") {
            ""
          } else {
            s"``${setterLines(rowNum)}``"
          }
        }
      val nextDocLine = if (rowNum >= docLinesLen) "" else docLines(rowNum)
      builder
        .append("\n")
        .append("|")
        .append("| ")
        .append(repeat(sizes.nameLength + 4, " "))
        .append(" | ")
        .append(repeat(sizes.valueLength, " "))
        .append(" | ")
        .append(nextSetterLine)
        .append(repeat(sizes.setterLength - nextSetterLine.length + 4, " "))
        .append(" | ")
        .append(nextDocLine)
        .append(repeat(sizes.docLength - nextDocLine.length, " "))
        .append(" |")
    }
    builder.toString()
  }

  private def lineSeparator(sizes: LineSizes, ch: String): String = {
    val builder = new StringBuilder
    // + 2 for the initial and ending space in the actual rows
    // + 4 for `` twice surrounding the option
    builder
      .append("+")
      .append(repeat(sizes.nameLength + 2 + 4, ch))
      .append("+")
      .append(repeat(sizes.valueLength + 2, ch))
      .append("+")
      .append(repeat(sizes.setterLength + 2 + 4, ch))
      .append("+")
      .append(repeat(sizes.docLength + 2, ch))
      .append("+")
    builder.toString()
  }

  private def repeat(n: Int, char: String): String = {
    List.fill(n)(char).mkString
  }
}
