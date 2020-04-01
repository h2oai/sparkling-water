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

package ai.h2o.sparkling.bench

import java.util.concurrent.TimeUnit

import org.scalatest.FunSuite

import scala.concurrent.duration.TimeUnit

class BenchSuite extends FunSuite {

  protected def benchTest(
      testName: String,
      iterations: Int = 5,
      warmUp: Int = 1,
      outputTimeUnit: TimeUnit = TimeUnit.MILLISECONDS)(testFun: => Unit): Unit = {
    def body: Unit = {
      val result = bench(iterations, warmUp, outputTimeUnit) {
        val evaluated = testFun
      }
      println(s"$testName: ${result.show()}")
    }

    registerTest(testName)(body)
  }

  /**
    * Measure execution time of given block in nanoseconds.
    *
    * @param block block to measure
    * @return number of ns to execute given block
    */
  def timer(block: => Unit): Long = {
    val now = System.nanoTime()
    block
    System.nanoTime() - now
  }

  /**
    * Benchmark given block of code.
    *
    * @param iterations number of iterations to execute the block of code
    * @param block      block to execute as benchmark
    * @return
    */
  def bench(iterations: Int, warmup: Int = 4, outputTimeUnit: TimeUnit = TimeUnit.MILLISECONDS)(
      block: => Unit): BenchResult = {
    val times = new Array[Long](iterations)
    // Warmup
    for (i <- 0 until warmup) {
      timer(block)
    }
    // Measure
    for (i <- 0 until iterations) {
      times(i) = timer(block)
    }

    BenchResult(times, TimeUnit.NANOSECONDS, outputTimeUnit)
  }
}
