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

package org.apache.spark.h2o.ui

import javax.servlet.http.HttpServletRequest
import org.apache.spark.h2o.SparkSpecificUtils
import org.apache.spark.ui.WebUIPage

import scala.xml.Node

/**
  * Sparkling Water H2O Flow Page
  */
case class SparklingWaterFlowUIPage(parent: SparklingWaterFlowUITab) extends WebUIPage("") {

  private val provider = parent.provider


  private def flowUrl(): String = s"http://${provider.localIpPort}"


  override def render(request: HttpServletRequest): Seq[Node] = {
    val helpText =
      """
        |Sparkling Water runtime information.
      """.stripMargin

    val content = if (provider.isSparklingWaterStarted) {
      <div>
        <iframe allowfullscreen="true"
                src={flowUrl()}
                style="margin: 0; border: 0; width: 100%; height: 100%;position:absolute;left:0;"
                frameborder="0">
        </iframe>
      </div>
    } else {
      <div>
        <h4>Sparkling Water UI not ready yet!</h4>
      </div>
    }
    SparkSpecificUtils.headerSparkPage(request, "H2O Flow", content, parent, helpText)
  }

}
