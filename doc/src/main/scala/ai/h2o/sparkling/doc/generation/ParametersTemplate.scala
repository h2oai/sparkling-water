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

object ParametersTemplate {
  def apply(entity: Class[_]): String = {
    val entities = getListOfAffectedEntities(entity)
    val caption = entities.map(_._1).mkString("Parameters of ", ", ", "")
    val dashes = caption.toCharArray.map(_ => '-').mkString
    val classes = entities.map(c => s"* ``$c``").mkString("\n")
    val tableContent = getParametersTableContent(entity)
    s"""$caption
       |$dashes
       |
       |**Classes:**
       |$classes
       |
       |.. csv-table:: a title
       |   :header: "Parameter", "Description"
       |   :widths: 20, 80
       |
       |$tableContent
     """.stripMargin
  }

  private def getListOfAffectedEntities(entity: Class[_]): Seq[(String, String)] = {
    val baseSimpleName = entity.getSimpleName
    val baseCanonicalName = entity.getCanonicalName
    val base = (baseSimpleName, baseCanonicalName) :: Nil
    val namespaceWithDot = baseCanonicalName.substring(baseCanonicalName.length - baseSimpleName.length)

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

  private def getParametersTableContent(entity: Class[_]): String = {
    val instance = entity.newInstance().asInstanceOf[Params]
    instance.params
      .map { param =>
        s"""   "${param.name}", "${param.doc}""""
      }
      .mkString("\n")
  }
}
