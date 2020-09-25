/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.h2o.sparkling.doc.generation

import org.apache.spark.ml.param.Params
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

object ParametersTemplate {
  def apply(algorithm: Class[_], mojoModel: Option[Class[_]]): String = {
    val entities = getListOfAffectedEntities(algorithm)
    val caption = s"Parameters of ${algorithm.getSimpleName}"
    val dashes = caption.toCharArray.map(_ => '-').mkString
    val classes = entities.map(c => s"- ``${c._2}``").mkString("\n")
    val classesCaption = if (entities.length > 1) "Affected Classes" else "Affected Class"
    val classesCaptionUnderLine = classesCaption.replaceAll(".", "#")
    val content = getParametersContent(algorithm, mojoModel)
    s""".. _parameters_${algorithm.getSimpleName}:
       |
       |$caption
       |$dashes
       |
       |$classesCaption
       |$classesCaptionUnderLine
       |
       |$classes
       |
       |Parameters
       |##########
       |
       |- *Each parameter has also a corresponding getter and setter method.*
       |  *(E.g.:* ``label`` *->* ``getLabel()`` *,* ``setLabel(...)`` *)*
       |
       |$content
     """.stripMargin
  }

  private def getListOfAffectedEntities(entity: Class[_]): Seq[(String, String)] = {
    val baseSimpleName = entity.getSimpleName
    val baseCanonicalName = entity.getCanonicalName
    val base = (baseSimpleName, baseCanonicalName) :: Nil
    val namespaceWithDot = baseCanonicalName.substring(0, baseCanonicalName.length - baseSimpleName.length)

    val fullClassifierName = s"${namespaceWithDot}classification.${baseSimpleName}Classifier"
    val withClassifier = Try(Class.forName(fullClassifierName)) match {
      case Success(classifier) => base :+ (classifier.getSimpleName, classifier.getCanonicalName)
      case Failure(_) => base
    }

    val fullRegressorName = s"${namespaceWithDot}regression.${baseSimpleName}Regressor"
    val withRegressor = Try(Class.forName(fullRegressorName)) match {
      case Success(regressor) => withClassifier :+ (regressor.getSimpleName, regressor.getCanonicalName)
      case Failure(_) => withClassifier
    }

    withRegressor
  }

  private def getParametersContent(algorithm: Class[_], mojoModel: Option[Class[_]]): String = {
    val algorithmInstance = algorithm.newInstance().asInstanceOf[Params]
    val mojoModelInstanceOption =
      mojoModel.map(_.getConstructor(classOf[String]).newInstance("uid").asInstanceOf[Params])
    algorithmInstance.params
      .filterNot(_.name == "withDetailedPredictionCol")
      .map { param =>
        val defaultValue = if (algorithmInstance.getDefault(param).isDefined) {
          algorithmInstance.getDefault(param).get
        } else {
          "No default value"
        }

        s"""${param.name}
           |  ${param.doc.replace("\n ", "\n\n  - ")}
           |
           |  ${generateDefaultValue(defaultValue)}
           |  ${generateMOJOComment(param.name, mojoModelInstanceOption)}
           |""".stripMargin
      }
      .mkString("\n")
  }

  private def generateMOJOComment(paramName: String, mojoModelInstanceOption: Option[Params]): String = {
    mojoModelInstanceOption
      .map { mojoModelInstance =>
        if (mojoModelInstance.hasParam(paramName)) {
          s"\n  *Also available on the trained model.*"
        } else {
          ""
        }
      }
      .getOrElse("")
  }

  private def generateDefaultValue(value: Any): String = {
    val pythonValue = stringifyAsPython(value)
    val scalaValue = stringifyAsScala(value)
    if (pythonValue == scalaValue) {
      s"*Default value:* ``$pythonValue``"
    } else {
      s"*Scala default value:* ``$scalaValue`` *; Python default value:* ``$pythonValue``"
    }
  }

  private def stringifyAsPython(value: Any): String = value match {
    case a: Array[_] => s"[${a.map(stringifyAsPython).mkString(", ")}]"
    case m: java.util.Map[_, _] =>
      m.asScala
        .map(entry => s"${stringifyAsPython(entry._1)} -> ${stringifyAsPython(entry._2)}")
        .mkString("{", ", ", "}")
    case b: Boolean => b.toString.capitalize
    case s: String => s""""$s""""
    case v if v == null => "None"
    case v: Enum[_] => s""""$v""""
    case v => v.toString
  }

  protected def stringifyAsScala(value: Any): String = value match {
    case f: java.lang.Float => s"${f.toString.toLowerCase}f"
    case d: java.lang.Double => d.toString.toLowerCase
    case l: java.lang.Long => s"${l}L"
    case m: java.util.Map[_, _] =>
      m.asScala
        .map(entry => s"${stringifyAsScala(entry._1)} -> ${stringifyAsScala(entry._2)}")
        .mkString("Map(", ", ", ")")
    case a: Array[_] => s"Array(${a.map(stringifyAsScala).mkString(", ")})"
    case s: String => s""""$s""""
    case v if v == null => null
    case v => v.toString
  }
}
