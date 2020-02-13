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

package ai.h2o.sparkling.extensions.serde

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.channels.{ByteChannel, Channels}

/**
  * This class was created to be able to pass [[InputStream]] into the [[water.AutoBuffer]] class as a constructor
  * parameter. [[water.AutoBuffer]]'s constructor accepting [[InputStream]] directly is not generic and accepts only
  * persistent streams.
  */
class InputStreamByteChannel(inputStream: InputStream) extends ByteChannel {
  val innerChannel = Channels.newChannel(inputStream)

  override def read(dst: ByteBuffer): Int = innerChannel.read(dst)

  override def write(src: ByteBuffer): Int = {
    throw new UnsupportedOperationException("This channel serves only for reading!")
  }

  override def isOpen(): Boolean = innerChannel.isOpen()

  override def close(): Unit = innerChannel.close()
}
