package ai.h2o.sparkling.doc.generation

import org.apache.spark.ml.param.Params

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object ModelDetailsTemplate {
  def apply(algorithm: Class[_], mojoModel: Class[_]): String = {
    val caption = s"Details of ${mojoModel.getSimpleName}"
    val dashes = caption.toCharArray.map(_ => '-').mkString
    val content = getModelDetailsContent(algorithm, mojoModel)
    s""".. _model_details_${mojoModel.getSimpleName}:
       |
       |$caption
       |$dashes
       |
       |$content
     """.stripMargin
  }

  private def getModelDetailsContent(algorithm: Class[_], mojoModel: Class[_]): String = {
    val algorithmInstance = algorithm.newInstance().asInstanceOf[Params]
    val mojoModelInstance = mojoModel.getConstructor(classOf[String]).newInstance("uid").asInstanceOf[Params]
    val mojoParamNames = mojoModelInstance.params.map(_.name)
    val algorithmParamNames = algorithmInstance.params.map(_.name)
    val relevantParamNames = mojoParamNames.diff(algorithmParamNames)
    if (relevantParamNames.nonEmpty) {
      relevantParamNames
        .map { paramName =>
          val param = mojoModelInstance.getParam(paramName)

          s"""get${param.name.capitalize}()
             |  ${param.doc}

             |""".stripMargin
        }
        .mkString("\n")
    } else {
      "No model detail methods."
    }
  }
}
