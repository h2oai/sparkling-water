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

package org.apache.spark.repl.h2o

import java.util.regex.Pattern

/**
  * Interpreter classloader which allows multiple interpreters to coexist
  */
class InterpreterClassLoader(val mainClassLoader: Option[ClassLoader]) extends ClassLoader {
  def this(mainClassLoader: ClassLoader) = this(Some(mainClassLoader))

  def this() = this(None)

  override def findClass(name: String): Class[_] = {
    if (name.startsWith("_intp_id")) {
      val matcher = Pattern.compile("intp_id_\\d+_").matcher(name)
      matcher.find()
      val intp_id = Integer.valueOf(matcher.group())
      H2OIMain.existingInterpreters.get(intp_id).get.classLoader.findClass(name)
    } else {
      if (mainClassLoader.isDefined) {
        mainClassLoader.get.loadClass(name)
      } else {
        super.findClass(name)
      }
    }
  }
}

