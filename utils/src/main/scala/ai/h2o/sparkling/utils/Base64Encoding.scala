package ai.h2o.sparkling.utils

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Base64

object Base64Encoding {
  val byteOrder = ByteOrder.LITTLE_ENDIAN

  def encode(data: Array[Byte]): String = {
    Base64.getEncoder.encodeToString(data)
  }

  def encode(data: Array[Int]): String = {
    val buffer = ByteBuffer.allocate(data.length * 4).order(byteOrder)
    data.foreach(columnIndex => buffer.putInt(columnIndex))
    Base64.getEncoder.encodeToString(buffer.array())
  }

  def decode(string: String): Array[Byte] = {
    Base64.getDecoder.decode(string)
  }

  def decodeToIntArray(string: String): Array[Int] = {
    val bytes = decode(string)
    val buffer = ByteBuffer.wrap(bytes).order(byteOrder).asIntBuffer
    val result = new Array[Int](buffer.remaining)
    buffer.get(result)
    result
  }
}
