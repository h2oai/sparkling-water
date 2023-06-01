package ai.h2o.sparkling.doc.generation

import ai.h2o.sparkling.ml.metrics.MetricsDescription
import org.apache.spark.ml.param.Params

object MetricsTemplate {
  def apply(metricsClass: Class[_]): String = {
    val caption = s"${metricsClass.getSimpleName} Class"
    val dashes = caption.toCharArray.map(_ => '-').mkString
    val content = getMetricsContent(metricsClass)
    val description = metricsClass.getAnnotation[MetricsDescription](classOf[MetricsDescription]).description()

    s""".. _metrics_${metricsClass.getSimpleName}:
       |
       |$caption
       |$dashes
       |
       |$description
       |
       |**Getter Methods**
       |
       |$content
       |""".stripMargin
  }

  private def getMetricsContent(metricsClass: Class[_]): String = {
    val metricsInstance = metricsClass.newInstance().asInstanceOf[Params]
    metricsInstance.params
      .map { param =>
        val lowerCaseName = param.name.toLowerCase
        val method = metricsClass.getMethods.find(_.getName.toLowerCase == "get" + lowerCaseName).get

        s"""${method.getName}()
           |  **Returns:** ${param.doc.replace("\n ", "\n\n  - ")}
           |
           |  ${generateType(method.getReturnType())}
           |""".stripMargin
      }
      .mkString("\n")
  }

  private def generateType(returnType: Class[_]): String = returnType.getSimpleName match {
    case "DataFrame" | "Dataset" => "*Type:* ``DataFrame``"
    case "String" => "*Scala type:* ``String``, *Python type:* ``string``, *R type:* ``character``"
    case "Double" | "double" => "*Scala type:* ``Double``, *Python type:* ``float``, *R type:* ``numeric``"
    case "Float" | "float" => "*Scala type:* ``Float``, *Python type:* ``float``, *R type:* ``numeric``"
    case "Long" | "long" => "*Scala type:* ``Long``, *Python type:* ``int``, *R type:* ``integer``"
  }
}
