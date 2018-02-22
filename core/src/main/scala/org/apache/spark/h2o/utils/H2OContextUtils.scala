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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.h2o.BuildInfo
import org.apache.spark.internal.Logging
import water.H2O
import water.fvec.Frame
import water.util.{GetLogsFromNode, Log, StringUtils}

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
  def downloadH2OLogs(destination: URI): URI = {
    val perNodeZipByteArray = H2O.CLOUD.members.zipWithIndex.map { case (node, i) =>
      // Skip nodes that aren't healthy, since they are likely to cause the entire process to hang.
      try {
        if (node.isHealthy) {
          val g = new GetLogsFromNode()
          g.nodeidx = i
          g.doIt()
          g.bytes
        }
        else {
          StringUtils.bytesOf("Node not healthy")
        }
      }
      catch {
        case e: Exception =>
          StringUtils.toBytes(e)
      }
    }

    val clientNodeByteArray = if (H2O.ARGS.client) {
      try {
        val g = new GetLogsFromNode
        g.nodeidx = -1
        g.doIt()
        g.bytes
      } catch {
        case e: Exception =>
          StringUtils.toBytes(e)
      }
    } else {
      null
    }

    val outputFileStem = "h2ologs_" + new SimpleDateFormat("yyyyMMdd_hhmmss").format(new Date)
    zipLogs(perNodeZipByteArray, clientNodeByteArray, outputFileStem, destination)
  }

  def downloadH2OLogs(destination: String): String = {
    downloadH2OLogs(new URI(destination))
    destination
  }

  /** Zip the H2O logs and store them to specified destination */
  private def zipLogs(results: Array[Array[Byte]], clientResult: Array[Byte], topDir: String, destination: URI): URI = {
    assert(H2O.CLOUD._memary.length == results.length, "Unexpected change in the cloud!")
    val l = results.map(_.length).sum
    val baos = new ByteArrayOutputStream(l)
    // Add top-level directory.
    val zos = new ZipOutputStream(baos)
    val zde = new ZipEntry(topDir + File.separator)
    zos.putNextEntry(zde)

    try {
      // Add zip directory from each cloud member.
      results.zipWithIndex.foreach { case (result, i) =>
        val filename = topDir + File.separator + "node" + i + "_" + H2O.CLOUD._memary(i).getIpPortString.replace(':', '_').replace('/', '_') + ".zip"
        val ze = new ZipEntry(filename)
        zos.putNextEntry(ze)
        zos.write(result)
        zos.closeEntry()
      }

      // Add zip directory from the client node.  Name it 'driver' since that's what Sparking Water users see.
      if (clientResult != null) {
        val filename = topDir + File.separator + "driver.zip"
        val ze = new ZipEntry(filename)
        zos.putNextEntry(ze)
        zos.write(clientResult)
        zos.closeEntry()
      }
      // Close the top-level directory.
      zos.closeEntry()
    } finally {
      // Close the full zip file.
      zos.close()
    }

    import java.io.FileOutputStream
    try {
      val outputStream = new FileOutputStream(destination.toString)
      try {
        baos.writeTo(outputStream)
      }
      finally {
        if (outputStream != null) outputStream.close()
      }
    }
    destination
  }


  def isRunningOnDatabricks(): Boolean = {
    try {
      Class.forName("com.databricks.backend.daemon.driver.DriverLocal")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

}
