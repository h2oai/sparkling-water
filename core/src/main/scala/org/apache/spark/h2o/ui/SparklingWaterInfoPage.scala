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

import ai.h2o.sparkling.backend.utils.AzureDatabricksUtils
import javax.servlet.http.HttpServletRequest
import org.apache.spark.h2o.SparkSpecificUtils
import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.xml.{Attribute, Node, Null, Text}

/**
  * Sparkling Water info page.
  */
case class SparklingWaterInfoPage(parent: SparklingWaterUITab) extends WebUIPage("") {

  private val provider = parent.provider

  private def h2oInfo(): Seq[(String, String)] = {
    val h2oBuildInfo = provider.H2OBuildInfo
    Seq(
      ("H2O Build Version", h2oBuildInfo.h2oBuildVersion),
      ("H2O Git Branch", h2oBuildInfo.h2oGitBranch),
      ("H2O Git SHA", h2oBuildInfo.h2oGitSha),
      ("H2O Git Describe", h2oBuildInfo.h2oGitDescribe),
      ("H2O Build By", h2oBuildInfo.h2oBuildBy),
      ("H2O Build On", h2oBuildInfo.h2oBuildOn))
  }

  private def flowUrl(): String = parent.provider.H2OClusterInfo.flowURL

  private def swProperties(): Seq[(String, String, String)] = provider.sparklingWaterProperties

  private def swInfo(): Seq[(String, String)] = {
    val cloudInfo = provider.H2OClusterInfo
    Seq(("Flow UI", flowUrl()), ("Nodes", cloudInfo.cloudNodes.mkString(","))) ++ cloudInfo.extraBackendInfo
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val helpText =
      """
        |Sparkling Water runtime information.
      """.stripMargin

    val content = if (provider.isSparklingWaterStarted) {

      val swInfoTable = UIUtils.listingTable(propertyHeader, h2oRowWithId, swInfo(), fixedWidth = true)
      val swPropertiesTable =
        UIUtils.listingTable(Seq("Name", "Value", "Documentation"), propertiesRow, swProperties(), fixedWidth = true)
      val h2oInfoTable = UIUtils.listingTable(propertyHeader, h2oRow, h2oInfo(), fixedWidth = true)
      val memoryInfo = UIUtils.listingTable(propertyHeader, h2oRow, provider.memoryInfo.map {
        case (n, m) => (n, "Free: " + m)
      }, fixedWidth = true)
      <div>
        <ul class="unstyled">
          <li>
            <strong>User:</strong>{parent.getSparkUser}
          </li>
          <li>
            <strong>Uptime:</strong>{
        UIUtils.formatDuration(provider.timeInMillis - provider.H2OClusterInfo.h2oStartTime)
      }
          </li>
          <li>
            <strong>Health:</strong>{if (provider.isCloudHealthy) "\u2714" else "\u2716"}
          </li>
          <li>
            <strong>Secured communication:</strong>{if (provider.H2OClusterInfo.cloudSecured) "\u2714" else "\u2716"}
          </li>
          <li>
            <strong>Nodes:</strong>{provider.H2OClusterInfo.cloudNodes.length}
          </li>
          <li>
            <strong>Memory Info:</strong>{memoryInfo}
          </li>
          <li>
            <a id="Flow UI Link" target="_blank" href={flowUrl()}>
              <strong>Flow UI</strong>
            </a>
          </li>
        </ul>
      </div>
      <span>
        <h4>Sparkling Water</h4>{swInfoTable}<h4>Sparkling Water Properties</h4>{swPropertiesTable}<h4>H2O Build Information</h4>
        {h2oInfoTable}
        {additionalScript()}
      </span>
    } else {
      <div>
        <h4>Sparkling Water UI not ready yet!</h4>
      </div>
    }
    SparkSpecificUtils.headerSparkPage(request, "Sparkling Water", content, parent, helpText)
  }

  private def additionalScript(): Seq[Node] = {
    if (AzureDatabricksUtils.isRunningOnAzureDatabricks(parent.parent.conf)) {
      val javaScript = scala.xml.Unparsed(
        s"""document.getElementById("Flow UI").innerHTML = window.location.protocol + "//" + window.location.hostname + "${flowUrl()}";
           |document.getElementById("Flow UI Link").href = "${flowUrl()}";""".stripMargin)
      <script type="text/javascript">{
        javaScript
      }</script>
    } else {
      Seq.empty
    }
  }

  private def propertyHeader = Seq("Name", "Value")

  private def propertiesRow(kv: (String, String, String)) = <tr>
    <td>
      {kv._1}
    </td> <td>
      {kv._2}
    </td> <td>
      {kv._3}
    </td>
  </tr>

  private def h2oRow(kv: (String, String)) = <tr>
    <td>
      {kv._1}
    </td> <td>
      {kv._2}
    </td>
  </tr>

  private def h2oRowWithId(kv: (String, String)) = <tr>
    <td>
      {kv._1}
    </td> {<td>
      {kv._2}
    </td> % Attribute(None, "id", Text(kv._1), Null)}
  </tr>
}
