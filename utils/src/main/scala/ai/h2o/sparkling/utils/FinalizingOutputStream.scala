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
