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

package water

import java.nio.channels.SocketChannel


object AutoBufferUtils {

  def getInt(ab: AutoBuffer): Int = ab.getInt

  def create(socketChannel: SocketChannel) = new AutoBuffer(socketChannel, false)

  def writeToChannel(ab: AutoBuffer, channel: SocketChannel): Unit = {
    ab.flipForReading()
    while (ab._bb.hasRemaining){
      channel.write(ab._bb)
    }
  }

  def clearForWriting(ab: AutoBuffer): Unit = ab.clearForWriting(H2O.MAX_PRIORITY)

  val BBP_BIG_SIZE = AutoBuffer.BBP_BIG._size
}
