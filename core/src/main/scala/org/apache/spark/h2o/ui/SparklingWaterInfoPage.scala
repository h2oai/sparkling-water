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

import java.io.File
import java.nio.file.Files
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.backends.SharedBackendConf
import org.apache.spark.ui.{UIUtils, WebUI, WebUIPage}
import org.apache.spark.util.Utils
import org.spark_project.jetty.server.Request
import org.spark_project.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}

import scala.util.Random
import scala.xml.Node

/**
  * Sparkling Water info page.
  */
case class SparklingWaterInfoPage(parent: SparklingWaterUITab) extends WebUIPage("") {

  private val listener = parent.listener
  // directory where the logs are copied
  private val logDir = Utils.createTempDir()

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

  private def zip(out: String, files: Iterable[String]) {
    import java.io.{BufferedInputStream, FileInputStream, FileOutputStream}
    import java.util.zip.{ZipEntry, ZipOutputStream}

    val zip = new ZipOutputStream(new FileOutputStream(out))

    files.foreach { name =>
      zip.putNextEntry(new ZipEntry(name))
      val in = new BufferedInputStream(new FileInputStream(name))
      var b = in.read()
      while (b > -1) {
        zip.write(b)
        b = in.read()
      }
      in.close()
      zip.closeEntry()
    }
    zip.close()
  }

  private def getLogFiles(logDir: File) = {
    if (logDir.exists && logDir.isDirectory) {
      logDir.listFiles.filter { f => f.isFile && f.getName.endsWith(".log") }
        .map(_.getAbsolutePath).toList
    } else {
      List[String]()
    }
  }

  /** Extract log level from the log file name */
  private def extractLogLevel(fileName: String) = {
    fileName.substring(fileName.lastIndexOf('-') + 1, fileName.lastIndexOf('.')).toUpperCase()
  }

  private def isHistoryServer = H2OContext.get().isEmpty

  /** Get correct URL based whether we run on the history server or not */
  private def getURL(end: String) = {
    if (isHistoryServer) {
      // id needs to be used only in case of history server. In case of Spark UI we are located in the context
      // of the specific application already
      s"/uid-${ID.id}/sparkling-water/${end}"
    } else {
      end
    }
  }

  private def flowUrl(): String = s"http://${listener.h2oCloudInfo.get.localClientIpPort}"

  private def downloadLogsUrl = getURL("logs/full_logs.zip")

  /** Static handler URL */
  private def downloadLogsUrlSuffix = {
    if (isHistoryServer) {
      s"/uid-${ID.id}/sparkling-water/logs"
    } else {
      "/sparkling-water/logs"
    }
  }

  private def logInfo() = {
    val files = getLogFiles(clientLogDir())
    files.filter(_.endsWith(".log")).map { name =>
      val f = new File(name)

      <a href={getURL("logs/" + f.getName)}>
        <strong>
          {extractLogLevel(f.getName)}
        </strong>
      </a>
    }
  }

  private def swProperties(): Seq[(String, String)] = listener.swProperties.get

  private def clientLogDir(): File = {
    new File(listener.swProperties.get.find(_._1 == SharedBackendConf.PROP_CLIENT_LOG_DIR._1).get._2)
  }

  private def swInfo(): Seq[(String, String)] = {
    val cloudInfo = listener.h2oCloudInfo.get
    Seq(
      ("Flow UI", flowUrl()),
      ("Nodes", cloudInfo.cloudNodes.mkString(","))
    ) ++ cloudInfo.extraBackendInfo
  }

  /** Create a handler for serving files from a static directory */
  private def createStaticHandler(resourceBase: File, path: String): ServletContextHandler = {
    val contextHandler = new ServletContextHandler() {
      override def doHandle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse): Unit = {
        // get log files
        val logFiles = getLogFiles(clientLogDir())
        if (logFiles.nonEmpty) {
          import java.nio.file.StandardCopyOption.REPLACE_EXISTING
          // copy logs to output directory
          logFiles.foreach { f =>
            val file = new File(f)
            Files.copy(file.toPath, new File(logDir, file.getName).toPath, REPLACE_EXISTING)
          }
          // re-zip logs with refresh as the logs can contain new data
          zip(new File(logDir, "full_logs.zip").getAbsolutePath, logFiles)
        }
        super.doHandle(target, baseRequest, request, response)
      }
    }
    contextHandler.setInitParameter("org.eclipse.jetty.servlet.Default.gzip", "false")
    val staticHandler = new DefaultServlet
    val holder = new ServletHolder(staticHandler)
    holder.setInitParameter("resourceBase", resourceBase.getAbsolutePath)
    contextHandler.setContextPath(path)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }


  private def attachHandler(): Unit = {
    val clazz = classOf[WebUI]
    val method = clazz.getDeclaredMethod("attachHandler", classOf[ServletContextHandler])
    method.invoke(parent.parent, createStaticHandler(logDir, downloadLogsUrlSuffix))
  }

  // if we are running on history server register the handler in the constructor
  if (isHistoryServer) {
    attachHandler()
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val helpText =
      """
        |Sparkling Water runtime information.
      """.stripMargin

    val content = if (listener.uiReady) {
      // register the handler when we have all information available. This is possible only when we run on
      // the Spark UI
      if (!isHistoryServer) {
        attachHandler()
      }

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
            <strong>Uptime:</strong>{UIUtils.formatDuration(listener.lastTimeHeadFromH2O - listener.h2oCloudInfo.get.h2oStartTime)}
          </li>
          <li>
            <strong>Health:</strong>{if (listener.cloudHealthy) "\u2714" else "\u2716"}
          </li>
          <li>
            <strong>Secured communication:</strong>{listener.h2oCloudInfo.map(p => if (p.cloudSecured) "\u2714" else "\u2716").getOrElse("\u2754")}
          </li>
          <li>
            <strong>Nodes:</strong>{listener.h2oCloudInfo.get.cloudNodes.length}
          </li>
          <li>
            <a href={flowUrl()}>
              <strong>Flow UI</strong>
            </a>
          </li>
          <li>
            <a href={downloadLogsUrl}>
              <strong>Download H2O Logs</strong>
            </a>
          </li>
          <li>
            <strong>See logs:</strong>{logInfo()}
          </li>
        </ul>
      </div>
        <span>
          <h4>Sparkling Water</h4>{swInfoTable}<h4>Sparkling Water Properties</h4>{swPropertiesTable}<h4>H2O Build Information</h4>{h2oInfoTable}
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

object ID {
  // unique id used to represent the current application
  // we can't use application ID since in case of History server, we need to register static handler
  // before we have the application id
  lazy val id = Math.abs(new Random().nextLong())
}
