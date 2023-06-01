package ai.h2o.sparkling.api.generation.r

import java.lang.reflect.Method

object ConfigurationTemplate extends ((Array[Method], Array[Method], Map[String, List[Int]], Class[_]) => String) {

  def apply(
      getters: Array[Method],
      setters: Array[Method],
      settersArityMap: Map[String, List[Int]],
      entity: Class[_]): String = {

    s"""#
       |if (!exists("ConfUtils.getOption", mode = "function")) source(file.path("R", "ConfUtils.R"))
       |
       |#' @export ${entity.getSimpleName}
       |${entity.getSimpleName} <- setRefClass("${entity.getSimpleName}", methods = list(
       |
       |    #
       |    # Getters
       |    #
       |
       |${generateGetters(getters)},
       |
       |    #
       |    # Setters
       |    #
       |
       |${generateSetters(setters, settersArityMap)}
       |))""".stripMargin
  }

  private def generateGetters(getters: Array[Method]): String = {
    getters.map(generateGetter).mkString(",\n\n")
  }

  private def generateSetters(setters: Array[Method], settersArityMap: Map[String, List[Int]]): String = {
    setters.map(generateSetter(_, settersArityMap)).mkString(",\n\n")
  }

  private def generateGetter(m: Method): String = {
    m.getReturnType match {
      case clz if clz.getName == "scala.Option" =>
        s"""    ${m.getName} = function() { ConfUtils.getOption(invoke(jconf, "${m.getName}")) }"""
      case _ => s"""    ${m.getName} = function() { invoke(jconf, "${m.getName}") }"""
    }
  }

  private def generateSetter(m: Method, settersArityMap: Map[String, List[Int]]): String = {
    val arities = settersArityMap(m.getName)
    val overloaded = arities.length > 1
    if (overloaded) {
      s"""    ${m.getName} = function(...) { invoke(jconf, "${m.getName}", ...); .self }"""
    } else {
      val arity = arities.head
      if (arity == 0) {
        s"""    ${m.getName} = function() { invoke(jconf, "${m.getName}"); .self }"""
      } else {
        val parameterNames = m.getParameters.map(_.getName)
        val parameters = parameterNames.mkString(",")
        s"""    ${m.getName} = function(${parameters}) { invoke(jconf, "${m.getName}", ${parameters}); .self }"""
      }
    }
  }
}
