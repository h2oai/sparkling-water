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
package water.api

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.scalatest.FunSuite
import water.fvec.{AppendableVec, Frame, NewChunk, Vec}
import water.munging.JoinMethod

import scala.collection.immutable.IndexedSeq
import scala.reflect.ClassTag

class SupportAPISuite extends FunSuite with SharedH2OTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  test("Test Join Support") {
    import TestUtils._
    val left = frame("name", vec(Array("A", "B", "C", "D"), 0, 1, 2, 3))
    left.add("age", vec(null, 12, 13, 14, 15))
    left.update()

    val right = frame("name", vec(Array("Y", "B", "X", "D"), 0, 1, 2, 3))
    right.add("salary", vec(null, 10000, 20000, 10000, 40000))
    right.update()

    import water.support.JoinSupport._
    import water.munging.JoinMethod._
    // Define test space
    type JOIN_TYPE = (Frame, Frame, JoinMethod) => Frame
    val NA = null.asInstanceOf[Int]

    val testSpace: Array[(String, JOIN_TYPE, Array[(JoinMethod, Boolean)], Array[(String, Int, Int)])] =
    // Join type, join method, enabled, expected result
      Array(("LEFT", leftJoin _,
        Array((RADIX, true), (HASH, true)),
        Array(("A", 12, NA), ("B", 13, 20000), ("C", 14, NA), ("D", 15, 40000))),
        ("RIGHT", rightJoin _,
          Array((RADIX, false), (HASH, true)),
          Array(("Y", NA, 10000), ("B", 13, 20000), ("X", NA, 10000), ("D", 15, 40000))),
        ("INNER", innerJoin _,
          Array((RADIX, true), (HASH, true)),
          Array(("B", 13, 20000), ("D", 15, 40000))),
        ("OUTER", outerJoin _,
          Array((RADIX, false), (HASH, false)),
          Array(("A", 12, NA), ("B", 13, 20000), ("C", 14, NA), ("D", 15, 40000), ("X", NA, 10000), ("Y", NA, 10000)))
      )
    println(testSpace.mkString("\n"))

    try {
      for (testComb <- testSpace) {
        val (joinName, joinType, methods, expected) = testComb
        for (method <- methods) {
          val (joinMethod, enabled) = method
          if (enabled) {
            println(s"${joinName} via ${joinMethod}")
            val result = joinType(left, right, joinMethod)
            println(result)
            assertFrameEqual("Data does not match", expected, result)
            result.remove()
          }
        }
      }
    } finally {
      left.remove()
      right.remove()
    }
  }
}

object TestUtils {

  def locate(name: String): File = {
    val abs = new File("/home/0xdiag/" + name)
    if (abs.exists()) {
      abs
    } else {
      new File("./examples/" + name)
    }
  }

  def frame(name: String, vec: Vec): Frame = {
    val f: Frame = new Frame(water.Key.make[Frame]())
    f.add(name, vec)
    water.DKV.put(f)
    return f
  }

  def vec(domain: Array[String], rows: Int*): Vec = {
    val k = Vec.VectorGroup.VG_LEN1.addVec
    val fs = new water.Futures
    val avec = new AppendableVec(k, Vec.T_NUM)
    avec.setDomain(domain)
    val chunk = new NewChunk(avec, 0)
    for (r <- rows) {
      chunk.addNum(r)
    }
    chunk.close(0, fs)
    val vec = avec.layout_and_close(fs)
    fs.blockForPending
    vec
  }

  implicit object TestJoinSupportConverter extends ((Frame, Int) => (String, Int, Int)) {

    override def apply(f: Frame,
                       idx: Int): (String, Int, Int) = {
      val (fName, fAge, fSalary) = (f.vec("name"), f.vec("age"), f.vec("salary"))
      val v1: String = if (fName.isNA(idx)) null else fName.domain()(fName.at8(idx).asInstanceOf[Int])
      val v2: Int = if (fAge.isNA(idx)) null.asInstanceOf[Int] else fAge.at8(idx).toInt
      val v3: Int = if (fSalary.isNA(idx)) null.asInstanceOf[Int] else fSalary.at8(idx).toInt
      (v1, v2, v3)
    }
  }

  def frameTo[T: ClassTag](f: Frame)(implicit converter: (Frame, Int) => T): Array[T] = {
    val nrow = f.numRows().asInstanceOf[Int]
    val result: IndexedSeq[T] = for (rowIdx <- 0 until nrow) yield converter(f, rowIdx)
    result.toArray[T]
  }

  def assertFrameEqual(msg: String, expected: Array[(String, Int, Int)], actual: Frame): Unit = {
    assert(3 == actual.numCols(), s"${msg}: Number of columns has to match")
    if (expected != null) {
      assert(expected.length == actual.numRows(), s"${msg}: Numer of rows has to match")
      val actualData = frameTo[(String, Int, Int)](actual).sortBy(_._1)
      val expectedData = expected.sortBy(_._1)
      expectedData.zip(actualData).foreach { case (exp, act) =>
        assert(exp == act, s"The rows have to match: ${expectedData.mkString(",")}\n!=\n${actualData.mkString(",")}")
      }
    }
  }
}
