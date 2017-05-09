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

import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.xml.Node

/**
  * Sparkling Water info page.
  */
case class SparklingWaterInfoPage(parent: SparklingWaterUITab) extends WebUIPage("") {

  private val listener = parent.listener

  private def h2oInfo(): Seq[(String, String)] = {
    val h2oBuildInfo = listener.h2oBuildInfo.get
    Seq(
      ("H2O Build Version", h2oBuildInfo.h2oBuildVersion),
      ("H2O Git Branch", h2oBuildInfo.h2oGitBranch),
      ("H2O Git SHA", h2oBuildInfo.h2oGitSha),
      ("H2O Git Describe", h2oBuildInfo.h2oGitDescribe),
      ("H2O Build By", h2oBuildInfo.h2oBuildBy),
      ("H2O Build On", h2oBuildInfo.h2oBuildOn)
    )
  }

  private def flowUrl(): String = s"http://${listener.h2oCloudInfo.get.localClientIpPort}"

  private def swProperties(): Seq[(String, String)] = listener.swProperties.get

  private def swInfo(): Seq[(String, String)] = {
    val cloudInfo = listener.h2oCloudInfo.get
    Seq(
      ("Flow UI", flowUrl()),
      ("Nodes", cloudInfo.cloudNodes.mkString(","))
    ) ++ cloudInfo.extraBackendInfo
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val helpText =
      """
        |Sparkling Water runtime information.
      """.stripMargin

    val content = if (listener.uiReady) {

      val swInfoTable = UIUtils.listingTable(
        propertyHeader, h2oRow, swInfo(), fixedWidth = true)
      val swPropertiesTable = UIUtils.listingTable(
        propertyHeader, h2oRow, swProperties(), fixedWidth = true)
      val h2oInfoTable = UIUtils.listingTable(
        propertyHeader, h2oRow, h2oInfo(), fixedWidth = true)


      <div>
        <ul class="unstyled">
          <li>
            <strong>User:</strong>{parent.getSparkUser}
          </li>
          <li>
            <strong>Uptime:</strong>
            {UIUtils.formatDuration(listener.lastTimeHeadFromH2O - listener.h2oCloudInfo.get.h2oStartTime)}
          </li>
          <li>
            <strong>Health:</strong>{if (listener.cloudHealthy) "\u2714" else "\u2716"}
          </li>
          <li>
            <strong>Nodes:</strong>
            {listener.h2oCloudInfo.get.cloudNodes.length}
          </li>
          <li>
            <a href={flowUrl()}>
              <strong>Flow UI</strong>
            </a>
          </li>
        </ul>
      </div>
        <span>
          <h4>Sparkling Water</h4> {swInfoTable}
          <h4>Sparkling Water Properties</h4> {swPropertiesTable}
          <h4>H2O Build Information</h4>{h2oInfoTable}
        </span>

    } else {
      <div>
        <h4>Sparkling Water UI not ready yet!</h4>
      </div>
    }

    UIUtils.headerSparkPage("Sparkling Water", content, parent, helpText = Some(helpText))

  }

  private def propertyHeader = Seq("Name", "Value")

  private def h2oRow(kv: (String, String)) = <tr>
    <td>
      {kv._1}
    </td> <td>
      {kv._2}
    </td>
  </tr>

}
