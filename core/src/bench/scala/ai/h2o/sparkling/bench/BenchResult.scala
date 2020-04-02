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

import scala.concurrent.duration.TimeUnit

case class BenchResult(mean: Float, stdDev: Float, min: Float, max: Float, unit: TimeUnit) {
  def show(): String = {
    f"$mean%4f ± $stdDev%4f ($min%4f, $max%4f)"
  }
}

object BenchResult {
  def apply(measurements: Array[Long], inputUnit: TimeUnit, outputUnit: TimeUnit): BenchResult = {
    val convMeasurements = measurements.map(x => outputUnit.convert(x, inputUnit))
    val mean = convMeasurements.sum.toFloat / convMeasurements.length
    val stdev =
      Math.sqrt(convMeasurements.map(x => (x - mean) * (x - mean)).sum / (convMeasurements.length - 1)).toFloat
    new BenchResult(mean, stdev, convMeasurements.min, convMeasurements.max, outputUnit)
  }
}
