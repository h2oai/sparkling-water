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
package water.fvec

/**
 * Simple bring to access package-private members.
 */
object FrameUtils {

  def preparePartialFrame(fr: Frame, names: Array[String]): Unit = {
    fr.preparePartialFrame(names)
  }

  def finalizePartialFrame(fr: Frame, rowsPerChunk: Array[Long],
                           colDomains: Array[Array[String]],
                           colTypes: Array[Byte]): Unit = {
    fr.finalizePartialFrame(rowsPerChunk, colDomains, colTypes)
  }

  def createNewChunks( name: String, cidx: Int ): Array[NewChunk] =
    Frame.createNewChunks(name, cidx)

  def closeNewChunks( nchks : Array[NewChunk] ): Unit = Frame.closeNewChunks(nchks)
}
