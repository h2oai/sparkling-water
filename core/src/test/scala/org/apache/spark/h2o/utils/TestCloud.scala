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

import org.junit.Assert._
import water.H2O

/**
  * Test fixture to control cloud
  * from ater.TestUtil in H2O module
  */
class TestCloud(size: Int, args: String*) extends TestBase {

  private lazy val isInitialized: Boolean = try {
    H2O.main(args.toArray)
    H2O.registerRestApis(System.getProperty("user.dir"))
    true
  } catch {
    case x => false
  }

  protected lazy val initialKeyCount: Int = H2O.store_size

  // Stall test until we see at least X members of the Cloud
  def stallTillCloudSize() {
    assertTrue("Failed to init H2O", isInitialized)
    H2O.waitForCloudSize(size, 30000)
    assertTrue("Weird number of entries in KVS", initialKeyCount >= 0)
  }
}
