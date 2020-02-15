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

package ai.h2o.sparkling.utils

import java.io.OutputStream

class FinalizingOutputStream(inner: OutputStream, finalizer: () => Unit) extends OutputStream {
  override def close(): Unit = {
    try {
      inner.close()
    } finally {
      finalizer()
    }
  }

  override def flush(): Unit = inner.flush()

  override def write(b: Int): Unit = inner.write(b)

  override def write(b: Array[Byte]): Unit = inner.write(b)

  override def write(b: Array[Byte], off: Int, len: Int): Unit = inner.write(b, off, len)
}
