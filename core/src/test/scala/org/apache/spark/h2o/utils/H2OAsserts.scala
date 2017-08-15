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

import water.parser.BufferedString

object H2OAsserts {
  def assertVectorIntValues(vec: water.fvec.Vec, values: Seq[Int]): Unit = {
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(if (vec.isNA(rIdx)) -1 == values(rIdx)
             else vec.at8(rIdx) == values(rIdx), "Values stored in H2OFrame has to match specified values")
    }
  }

  def assertVectorDoubleValues(vec: water.fvec.Vec, values: Seq[Double]): Unit = {
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(if (vec.isNA(rIdx)) values(rIdx) == Double.NaN // this is Scala i can do NaN comparision
             else vec.at(rIdx) == values(rIdx), "Values stored in H2OFrame has to match specified values")
    }
  }

  def assertVectorEnumValues(vec: water.fvec.Vec, values: Seq[String]): Unit = {
    val vecDom = vec.domain()
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(vecDom(vec.at8(rIdx).asInstanceOf[Int]) == values(rIdx), "Values stored in H2OFrame has to match specified values")
    }
  }

  def assertVectorStringValues(vec: water.fvec.Vec, values: Seq[String]): Unit = {
    val valString = new BufferedString()
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(
        vec.isNA(rIdx) || {
          vec.atStr(valString, rIdx)
          valString.toSanitizedString == values(rIdx)
        }, "Values stored in H2OFrame has to match specified values")
    }
  }

  def assertDoubleFrameValues(f: water.fvec.Frame, rows: Seq[Array[Double]]): Unit = {
    val ncol = f.numCols()
    val rowsIdx = (0 until f.numRows().toInt)
    val columns = (0 until ncol).map(cidx => rowsIdx.map(rows(_)(cidx)))
    f.vecs().zipWithIndex.foreach { case (vec, idx: Int) =>
      assertVectorDoubleValues(vec, columns(idx))
    }
  }
}
