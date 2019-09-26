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

package org.apache.spark.h2o.backends.external

import java.net.URI
import org.apache.spark.h2o.utils.{H2OContextRestAPIUtils, NodeDesc}

trait ExternalBackendRestApiUtils extends H2OContextRestAPIUtils {

  protected def waitForCloudSizeViaRestAPI(endpoint: URI, extected: Int, timeout: Long): Array[NodeDesc] = {
    val start = System.currentTimeMillis()
    val cloudV3 = getCloudInfo(endpoint)

    while (System.currentTimeMillis() - start < timeout) {
      if (getNodes(cloudV3).length < extected || !cloudV3.consensus) {
        try {
          Thread.sleep(100);
        } catch {
          case ignored: InterruptedException =>
        }
      } else {
        return getNodes(cloudV3)
      }
    }

    val nodes = getNodes(cloudV3)
    if (nodes.length < extected) {
      throw new RuntimeException("Cloud size " + nodes.length + " under " + extected);
    }

    nodes
  }
}
