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

package org.apache.spark.h2o

import water.fvec.{Chunk, NewChunk, Vec}
import water.nbhm.NonBlockingHashMap
import water.parser.BufferedString
import water.{AutoBuffer, MRTask}

/**
 * Simple vector utilities.
 */
@deprecated("Will be removed in future version (with upgrade of H2O dependency). Please do not use it.", "NA")
object VecUtils {

  def stringToCategorical(vec : Vec): Vec = {
    val domain = new CollectVecDomain().domain(vec)
    val task = new MRTask() {
      @transient
      private var lookupTable: java.util.HashMap[String, Int] = _

      override def setupLocal(): Unit = {
        lookupTable = new java.util.HashMap[String, Int]()
        domain.indices.foreach (idx => lookupTable.put(domain(idx), idx))
      }

      override def map(c: Chunk, nc: NewChunk): Unit = {
        val vs = new BufferedString()
        (0 until c.len()).foreach { rIdx =>
          if (c.isNA(rIdx)) nc.addNA()
          else {
            c.atStr(vs, rIdx)
            nc.addCategorical(lookupTable.get(vs.bytesToString()))
          }
        }
      }
    }
    // Invoke tasks - one input vector, one ouput vector
    task.doAll(Array[Byte](Vec.T_CAT), vec)
    // Return result
    task.outputFrame(null, null, Array(domain)).vec(0)
  }

  /** Collect string column domain. */
  class CollectVecDomain extends MRTask[CollectVecDomain] {

    @transient
    private var _uniques: NonBlockingHashMap[String, Object] = null

    override def setupLocal(): Unit = {
      _uniques = new NonBlockingHashMap[String, Object]()
    }

    override def map(c: Chunk): Unit = {
      val vs = new BufferedString()
      (0 until c.len()).foreach(rIdx => {
        if (!c.isNA(rIdx)) {
          c.atStr(vs, rIdx)
          _uniques.put(vs.bytesToString(), "")
        }
      })
    }

    override def reduce(mrt: CollectVecDomain): Unit = {
      if (_uniques != mrt._uniques) { // this is not local reduce
        _uniques.putAll(mrt._uniques)
      }
    }

    override def write_impl(ab: AutoBuffer): AutoBuffer = {
      return ab.putAStr(if (_uniques == null) null else _uniques.keySet().toArray(new Array[String](_uniques.size())))
    }

    override def read_impl(ab: AutoBuffer): CollectVecDomain = {
      val arys = ab.getAStr()
      _uniques = new NonBlockingHashMap[String, Object]()
      if (arys != null) {
        arys.map(value => _uniques.put(value, ""))
      }
      return this
    }


    override def copyOver(src: CollectVecDomain): Unit = {
      _uniques = src._uniques
    }

    def domain(vec : Vec): Array[String] = {
      this.doAll(vec)
      domain()
    }

    def domain() : Array[String] = {
      val dom: Array[String] = _uniques.keySet().toArray(new Array[String](_uniques.size()))
      scala.util.Sorting.quickSort(dom)
      dom
    }
  }
}
