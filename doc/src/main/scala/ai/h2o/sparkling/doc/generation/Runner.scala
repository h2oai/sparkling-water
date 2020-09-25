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

import java.io.{File, PrintWriter}
import java.net.URLDecoder
import java.util.jar.JarFile

import ai.h2o.sparkling.utils.ScalaUtils.withResource

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object Runner {
  private def writeResultToFile(content: String, fileName: String, destinationDir: String) = {
    val destinationDirFile = new File(destinationDir)
    destinationDirFile.mkdirs()
    val destinationFile = new File(destinationDirFile, s"$fileName.rst")
    withResource(new PrintWriter(destinationFile)) { outputStream =>
      outputStream.print(content)
    }
  }

  def main(args: Array[String]): Unit = {
    val destinationDir = args(0)
    val algorithms = getParamClasses("ai.h2o.sparkling.ml.algos")
    val featureTransformers = getParamClasses("ai.h2o.sparkling.ml.features")
    writeResultToFile(ParametersTocTreeTemplate(algorithms, featureTransformers), "parameters", destinationDir)
    for (algorithm <- algorithms) {
      val modelClassName = s"ai.h2o.sparkling.ml.models.${algorithm.getSimpleName}MOJOModel"
      val model = Try(Class.forName(modelClassName)).toOption
      val content = ParametersTemplate(algorithm, model)
      writeResultToFile(content, s"parameters_${algorithm.getSimpleName}", destinationDir)
    }
    for (featureTransformer <- featureTransformers) {
      val modelClassName = s"ai.h2o.sparkling.ml.models.${featureTransformer.getSimpleName}MOJOModel"
      val model = Try(Class.forName(modelClassName)).toOption
      writeResultToFile(
        ParametersTemplate(featureTransformer, model),
        s"parameters_${featureTransformer.getSimpleName}",
        destinationDir)
    }
  }

  private def getParamClasses(packageName: String): Array[Class[_]] = {
    val classLoader = Thread.currentThread.getContextClassLoader
    val path = packageName.replace('.', '/')
    val packageUrl = classLoader.getResource(path)
    val classes = new ArrayBuffer[Class[_]]
    if (packageUrl.getProtocol().equals("jar")) {
      val decodedUrl = URLDecoder.decode(packageUrl.getFile(), "UTF-8")
      val jarFileName = decodedUrl.substring(5, decodedUrl.indexOf("!"))
      val jarFile = new JarFile(jarFileName)
      val entries = jarFile.entries()
      while (entries.hasMoreElements) {
        val entryName = entries.nextElement().getName
        if (entryName.startsWith(path) && entryName.endsWith(".class")) {
          val name = entryName.substring(path.length + 1, entryName.length - 6)
          if (!name.contains('/')) {
            classes.append(Class.forName(packageName + '.' + name))
          }
        }
      }
    } else {
      val directory = new File(packageUrl.toURI)
      val files = directory.listFiles
      for (file <- files) {
        if (file.getName.endsWith(".class")) {
          classes.append(Class.forName(packageName + '.' + file.getName.substring(0, file.getName.length - 6)))
        }
      }
    }
    classes.filter(_.getConstructors().size == 2).toArray
  }
}
