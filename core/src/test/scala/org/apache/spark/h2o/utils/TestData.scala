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

import java.sql.Timestamp

import org.apache.spark.mllib

/**
  * Created by vpatryshev on 8/29/16.
  */
object TestData {
  // Helper classes for conversion from RDD to DataFrame
  // which expects T <: Product
  case class ByteField   (v: Byte)
  case class ShortField  (v: Short)
  case class IntField    (v: Int)
  case class LongField   (v: Long)
  case class FloatField  (v: Float)
  case class DoubleField (v: Double)
  case class StringField(v: String)
  case class TimestampField(v: Timestamp)

  case class PrimitiveA(n: Int, name: String)
  case class ComposedA(a: PrimitiveA, weight: Double)
  case class ComposedWithTimestamp(a: PrimitiveA, v: TimestampField)

  case class PrimitiveB(f: Seq[Int])

  case class PrimitiveC(f: mllib.linalg.Vector)

  case class Prostate(ID      :Option[Long]  ,
                      CAPSULE :Option[Int]  ,
                      AGE     :Option[Int]  ,
                      RACE    :Option[Int]  ,
                      DPROS   :Option[Int]  ,
                      DCAPS   :Option[Int]  ,
                      PSA     :Option[Float],
                      VOL     :Option[Float],
                      GLEASON :Option[Int]  ) {
    def isWrongRow():Boolean = (0 until productArity).map( idx => productElement(idx)).forall(e => e==None)
  }

  class PUBDEV458Type(val result: Option[Int]) extends Product with Serializable {
    override def canEqual(that: Any):Boolean = that.isInstanceOf[PUBDEV458Type]
    override def productArity: Int = 1
    override def productElement(n: Int) =
      n match {
        case 0 => result
        case _ => throw new IndexOutOfBoundsException(n.toString)
      }
  }

  case class OptionAndNot(val x: Option[Int], val y: Option[Int]) extends Serializable { self =>
//    override def productElement(n: Int) = this.
//      n match {
//        case 0 => x
//        case 1 => y
//        case _ => throw new IndexOutOfBoundsException(s"${self.getClass} has just 2 fields, asking for field ${n} is wrong")
//      }
  }
}
