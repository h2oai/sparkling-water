package ai.h2o.sparkling.ml.params

import java.util

import org.apache.spark.ml.param.{Param, Params}
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JValue}
import water.AutoBuffer

import scala.collection.JavaConverters._

class HyperParamsParam(
    parent: Params,
    name: String,
    doc: String,
    isValid: java.util.Map[String, Array[AnyRef]] => Boolean)
  extends Param[java.util.Map[String, Array[AnyRef]]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def jsonEncode(value: java.util.Map[String, Array[AnyRef]]): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      val ab = new AutoBuffer()
      ab.put1(value.size)
      val it = value.entrySet().iterator()
      while (it.hasNext) {
        val entry = it.next()
        ab.putStr(entry.getKey)
        //
        //noinspection ComparingUnrelatedTypes
        if (entry.getValue.isInstanceOf[util.ArrayList[Object]]) {
          val length = entry.getValue.asInstanceOf[util.ArrayList[_]].size()
          val arrayList = entry.getValue.asInstanceOf[util.ArrayList[_]]
          val arr = (0 until length).map(idx => arrayList.get(idx).asInstanceOf[AnyRef]).toArray
          ab.putASer(arr)
        } else {
          ab.putASer(entry.getValue)
        }
      }

      val bytes = ab.buf()
      JArray(bytes.toSeq.map(JInt(_)).toList)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): java.util.Map[String, Array[AnyRef]] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        val bytes = values.map {
          case JInt(x) =>
            x.byteValue()
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to Byte.")
        }.toArray
        val ab = new AutoBuffer(bytes)
        val numParams = ab.get1()
        (0 until numParams)
          .map { _ =>
            (ab.getStr, ab.getASer[AnyRef](classOf[AnyRef]))
          }
          .toMap
          .asJava
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Map[String, Array[AnyRef]].")
    }
  }
}
