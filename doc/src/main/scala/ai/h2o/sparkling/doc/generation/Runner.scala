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

import ai.h2o.sparkling.utils.ScalaUtils.withResource

import scala.collection.mutable.ArrayBuffer

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
    writeResultToFile(ParametersTocTreeTemplate(algorithms), "parameters", destinationDir)
    for (algorithm <- algorithms) {
      writeResultToFile(ParametersTemplate(algorithm), s"parameters_${algorithm.getSimpleName}", destinationDir)
    }
  }

  private def getParamClasses(packageName: String): Seq[Class[_]] = {
    val classLoader = Thread.currentThread.getContextClassLoader
    val path = packageName.replace('.', '/')
    val resources = classLoader.getResources(path)
    val directories = new ArrayBuffer[File]()
    while (resources.hasMoreElements) {
      val resource = resources.nextElement
      directories.append(new File(resource.getFile()))
    }
    val classes = new ArrayBuffer[Class[_]]
    for (directory <- directories) {
      classes.append(findClasses(directory, packageName): _*)
    }
    val paramsClass = Class.forName("org.apache.spark.ml.param.Param")
    classes.filter(paramsClass.isAssignableFrom(_))
  }

  private def findClasses(directory: File, packageName: String): Seq[Class[_]] = {
    val classes = new ArrayBuffer[Class[_]]
    if (!directory.exists) return classes
    val files = directory.listFiles
    for (file <- files) {
      if (file.getName.endsWith(".class")) {
        classes.append(Class.forName(packageName + '.' + file.getName.substring(0, file.getName.length - 6)))
      }
    }
    classes
  }
}
