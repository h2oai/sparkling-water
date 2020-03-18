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

package ai.h2o.sparkling.backend.utils

class ArgumentBuilder() {
  private var arguments = Seq.empty[String]

  def add(arg: String, value: Option[Any]): ArgumentBuilder = {
    if (value.isDefined) {
      arguments = arguments ++ Seq(arg, value.get.toString)
    }
    this
  }

  def add(args: Seq[String]): ArgumentBuilder = {
    arguments = arguments ++ args
    this
  }

  def add(args: Seq[String], condition: Boolean): ArgumentBuilder = {
    if (condition) {
      add(args)
    }
    this
  }

  def add(arg: String, condition: Boolean): ArgumentBuilder = {
    if (condition) {
      add(arg)
    }
    this
  }

  def add(value: Option[Any]): ArgumentBuilder = {
    if (value.isDefined) {
      arguments = arguments ++ Seq(value.get.toString)
    }
    this
  }

  def add(arg: String, value: String): ArgumentBuilder = {
    arguments = arguments ++ Seq(arg, value.toString)
    this
  }

  def add(arg: String, value: Int): ArgumentBuilder = {
    add(arg, value.toString)
  }

  def add(arg: String): ArgumentBuilder = {
    addIf(arg, condition = true)
  }

  def addIf(arg: String, value: String, condition: Boolean): ArgumentBuilder = {
    if (condition) {
      add(arg, value)
    } else {
      this
    }
  }

  def addIf(arg: String, value: Option[String], condition: Boolean): ArgumentBuilder = {
    if (condition) {
      add(arg, value.get)
    } else {
      this
    }
  }

  def addIf(arg: String, condition: Boolean): ArgumentBuilder = {
    if (condition) {
      arguments = arguments ++ Seq(arg)
    }
    this
  }

  def addAsString(args: String): ArgumentBuilder = {
    val array = args.split("\\s+")
    add(array)
  }

  def addAsString(argsOption: Option[String]): ArgumentBuilder = argsOption match {
    case Some(args) => addAsString(args)
    case None => this
  }

  def buildArgs(): Seq[String] = {
    arguments
  }

}
