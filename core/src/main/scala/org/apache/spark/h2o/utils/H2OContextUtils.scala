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

package org.apache.spark.h2o.utils

import java.net.{InetAddress, InetSocketAddress, ServerSocket, URI}

import org.apache.spark.expose.Logging
import org.apache.spark.h2o.{BuildInfo, H2OConf}
import org.apache.spark.{SparkContext, SparkEnv}
import org.eclipse.jetty.proxy.ProxyServlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHandler}
import water.H2O
import water.api.ImportHiveTableHandler
import water.api.ImportHiveTableHandler.HiveTableImporter
import water.fvec.Frame
import water.util.{Log, LogArchiveContainer}

import scala.language.postfixOps

/**
  * Support methods for H2OContext.
  */
private[spark] trait H2OContextUtils extends Logging {

  /**
    * Open browser for given address.
    *
    * @param uri address to open in browser, e.g., http://example.com
    */
  def openURI(sc: SparkContext, uri: String): Unit = {
    import java.awt.Desktop
    if (!isTesting(sc)) {
      if (Desktop.isDesktopSupported) {
        Desktop.getDesktop.browse(new java.net.URI(uri))
      } else {
        logWarning(s"Desktop support is missing! Cannot open browser for $uri")
      }
    }
  }

  private def isTcpPortAvailable(port: Int): Boolean = {
    try {
      val serverSocket = new ServerSocket()

      serverSocket.setReuseAddress(false);
      serverSocket.bind(new InetSocketAddress(InetAddress.getByName(SparkEnv.get.blockManager.blockManagerId.host), port), 1)
      serverSocket.close()
      true
    } catch {
      case _: Exception => false
    } finally {
    }
  }

  private def findNextFreeFlowPort(conf: H2OConf): Int = {
    if (conf.clientWebPort == -1) {
      var port = conf.clientBasePort
      val offset = conf.internalPortOffset
      while (!isTcpPortAvailable(port)) {
        port = port + offset + 1
      }
      port
    } else {
      val port = conf.clientWebPort
      if (!isTcpPortAvailable(port)) {
        throw new RuntimeException(s"Port $port not available!")
      } else {
        port
      }

    }
  }

  protected def startFlowProxy(conf: H2OConf): URI = {
    val port = findNextFreeFlowPort(conf)
    val server = new Server(port)

    val context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");

    val handler = new ServletHandler()
    val holder = handler.addServletWithMapping(classOf[ProxyServlet.Transparent], "/*")

    val cloudV3 = H2OContextRestAPIUtils.getCloudInfo(conf)
    val ipPort = cloudV3.nodes(cloudV3.leader_idx).ip_port
    
    holder.setInitParameter("proxyTo", s"${conf.getScheme()}://${ipPort}")
    holder.setInitParameter("prefix", "/")
    context.setServletHandler(handler)
    server.setHandler(context)
    server.start()
    while (!server.isStarted) {
      Thread.sleep(100)
    }

    new URI(s"${conf.getScheme()}://${SparkEnv.get.blockManager.blockManagerId.host}:$port")
  }

  /**
    * Return true if running inside spark/sparkling water test.
    *
    * @param sc Spark Context
    * @return true if the actual run is test run
    */
  def isTesting(sc: SparkContext) = sc.conf.contains("spark.testing") || sys.props.contains("spark.testing")

  /** Checks whether version of provided Spark is the same as Spark's version designated for this Sparkling Water version.
    * We check for correct version in shell scripts and during the build but we need to do the check also in the code in cases when the user
    * executes for example spark-shell command with sparkling water assembly jar passed as --jars and initiates H2OContext.
    * (Because in that case no check for correct Spark version has been done so far.)
    */
  def isRunningOnCorrectSpark(sc: SparkContext) = sc.version.startsWith(BuildInfo.buildSparkMajorVersion)


  def withConversionDebugPrints[R <: Frame](sc: SparkContext, conversionName: String, block: => R): R = {
    val propName = "spark.h2o.measurements.timing"
    val performancePrintConf = sc.getConf.getOption(propName).orElse(sys.props.get(propName))

    if (performancePrintConf.nonEmpty && performancePrintConf.get.toBoolean) {
      val t0 = System.nanoTime()
      val result = block
      val t1 = System.nanoTime()
      Log.info(s"Elapsed time of the ${conversionName} conversion into H2OFrame ${result._key}: " + (t1 - t0) / 1000 + " millis")
      result
    } else {
      block
    }
  }

  /**
    * @param destination directory where the logs will be downloaded
    */
  def downloadH2OLogs(destination: URI, logContainer: LogArchiveContainer): URI = {
    H2O.downloadLogs(destination, logContainer)
  }

  def downloadH2OLogs(destination: URI, logContainer: String): URI = {
    H2O.downloadLogs(destination, logContainer)
  }

  def downloadH2OLogs(destination: String, logContainer: LogArchiveContainer): String = {
    H2O.downloadLogs(destination, logContainer).toString
  }

  def downloadH2OLogs(destination: String, logContainer: String = "ZIP"): String = {
    H2O.downloadLogs(destination, logContainer).toString
  }

  def importHiveTable(database: String = HiveTableImporter.DEFAULT_DATABASE, table: String,
                      partitions: Array[Array[String]] = null, allowMultiFormat: Boolean = false): Frame = {
    val hiveTableHandler = new ImportHiveTableHandler
    val method = hiveTableHandler.getClass.getDeclaredMethod("getImporter")
    method.setAccessible(true)
    val importer = method.invoke(hiveTableHandler).asInstanceOf[ImportHiveTableHandler.HiveTableImporter]

    if (importer != null) {
      try {
        importer.loadHiveTable(database, table, partitions, allowMultiFormat).get()
      }
      catch {
        case e: NoClassDefFoundError =>
          throw new IllegalStateException("Hive Metastore client classes not available on classpath.", e)
      }
    } else {
      throw new IllegalStateException("HiveTableImporter extension not enabled.")
    }
  }

}
