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

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import water.fvec.Frame

private[utils] trait H2OContextUtils extends RestCommunication with RestApiUtils {

  protected def withConversionDebugPrints[R <: Frame](sc: SparkContext, conversionName: String, block: => R): R = {
    val propName = "spark.h2o.measurements.timing"
    val performancePrintConf = sc.getConf.getOption(propName).orElse(sys.props.get(propName))

    if (performancePrintConf.nonEmpty && performancePrintConf.get.toBoolean) {
      val t0 = System.nanoTime()
      val result = block
      val t1 = System.nanoTime()
      logInfo(s"Elapsed time of the $conversionName conversion into H2OFrame ${result._key}: " + (t1 - t0) / 1000 + " millis")
      result
    } else {
      block
    }
  }

  /**
   * Open browser for given address.
   *
   * @param uri address to open in browser, e.g., http://example.com
   */
  protected def openURI(uri: String): Unit = {
    import java.awt.Desktop
    if (Desktop.isDesktopSupported) {
      Desktop.getDesktop.browse(new java.net.URI(uri))
    } else {
      logWarning(s"Desktop support is missing! Cannot open browser for $uri")
    }
  }

  protected def logFileName(): String = {
    val pattern = "yyyyMMdd_hhmmss"
    val formatter = new SimpleDateFormat(pattern)
    val now = formatter.format(new Date)
    s"h2ologs_$now"
  }

  protected def verifyLogContainer(logContainer: String): Unit = {
    if (!Seq("ZIP", "LOG").contains(logContainer)) {
      throw new IllegalArgumentException(s"Supported LOG container is either LOG or ZIP, specified was: $logContainer")
    }
  }
}
