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

// Phase timings for the test scripts; the runners load this file before each script.
val timings = scala.collection.mutable.LinkedHashMap[String, Double]()

def timed[T](phase: String)(body: => T): T = {
  val start = System.nanoTime()
  try body
  finally timings(phase) = math.round((System.nanoTime() - start) / 1e6) / 1e3
}

def timingsJson: String = timings.map { case (phase, seconds) => s""""$phase": $seconds""" }.mkString("{", ", ", "}")
