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

import java.util.Scanner

/**
  * Interpreter classloader which allows multiple interpreters to coexist
  */
class InterpreterClassLoader(val fallbackClassloader: ClassLoader) extends ClassLoader {

  override def loadClass(name: String): Class[_] = {
    if (name.startsWith("intp_id")) {
      val intp_id = new Scanner(name).useDelimiter("\\D+").nextInt()
      H2OIMain.existingInterpreters(intp_id).classLoader.loadClass(name)
    } else {
      fallbackClassloader.loadClass(name)
    }
  }
}

