package org.apache.spark.expose

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

import java.io.File

import org.apache.spark.SparkConf

object ReplUtils {

  // Spark 2.2.0 behaves differently then Spark versions 2.2.1+. First try to handle Spark 2.2.1+ and fallback
  // to Spark 2.2.0
  def getJars(conf: SparkConf): String = {
    import scala.reflect.runtime.{universe => ru}
    val instanceMirror = ru.runtimeMirror(this.getClass.getClassLoader).reflect(org.apache.spark.util.Utils)
    val methodSymbol = ru.typeOf[org.apache.spark.util.Utils.type].decl(ru.TermName("getLocalUserJarsForShell"))

    val jars = if (methodSymbol.isMethod) {
      val method = instanceMirror.reflectMethod(methodSymbol.asMethod)
      method(conf).asInstanceOf[Seq[String]]
    } else {
      val m = ru.runtimeMirror(this.getClass.getClassLoader)
      val instanceMirror = m.reflect(org.apache.spark.util.Utils)
      val methodSymbol = ru.typeOf[org.apache.spark.util.Utils.type].decl(ru.TermName("getUserJars")).asMethod
      val method = instanceMirror.reflectMethod(methodSymbol)
      method(conf, true).asInstanceOf[Seq[String]]
    }
    jars.mkString(File.pathSeparator)
  }
}
