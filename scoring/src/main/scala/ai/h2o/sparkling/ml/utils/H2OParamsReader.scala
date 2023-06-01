package ai.h2o.sparkling.ml.utils

import org.apache.spark.ExposeUtils
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.expose.DefaultParamsReader.Metadata
import org.json4s.JsonAST
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.{compact, render}

private[ml] trait H2OParamsReader[T] {
  private def getAndSetParams(instance: Params, metadata: Metadata, skipParams: List[String]): Unit = {
    metadata.params match {
      case JObject(pairs) =>
        pairs.foreach {
          case (paramName, jsonValue) =>
            if (!skipParams.contains(paramName)) {
              val param = instance.getParam(paramName)
              val value = param.jsonDecode(compact(render(jsonValue)))
              instance.set(param, value)
            }
        }
      case _ =>
        throw new IllegalArgumentException(s"Cannot recognize JSON metadata: ${metadata.metadataJson}.")
    }
  }

  def load(metadata: Metadata): T = {
    val cls = ExposeUtils.classForName(metadata.className)
    val instance = cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]

    val parsedParams = metadata.params.asInstanceOf[JsonAST.JObject].obj.map(_._1)
    val allowedParams = instance.params.map(_.name)
    val skippedParams = parsedParams.diff(allowedParams)

    getAndSetParams(instance, metadata, skippedParams)
    instance.asInstanceOf[T]
  }
}
