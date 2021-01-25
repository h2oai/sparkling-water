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

import ai.h2o.sparkling.H2OFrame

import scala.collection.mutable.ArrayBuffer

trait H2OFrameLifecycle {
  private val h2oFramesToBeDeleted = new ArrayBuffer[H2OFrame]()

  private[sparkling] final def registerH2OFrameForDeletion(frame: H2OFrame): Unit = h2oFramesToBeDeleted.append(frame)

  private[sparkling] final def registerH2OFrameForDeletion(frame: Option[H2OFrame]): Unit = {
    frame.foreach(registerH2OFrameForDeletion)
  }

  private[sparkling] final def deleteRegisteredH2OFrames(): Unit = {
    h2oFramesToBeDeleted.foreach(_.delete())
    h2oFramesToBeDeleted.clear()
  }
}
