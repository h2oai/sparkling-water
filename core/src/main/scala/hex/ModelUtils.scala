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

package hex

import ai.h2o.sparkling.macros.DeprecatedMethod
import hex.Model.Output
import org.apache.spark.expose.Logging
import water.util.ArrayUtils

/**
  * Helper class to access package-private methods of Model API.
  */
object ModelUtils extends Logging {
  @DeprecatedMethod("Sparkling Water Algorithm API to train and score H2O models", "3.32")
  def classify(row: Array[Double], m: Model[_, _, _]): (String, Array[Double]) = {
    val modelOutput = m._output.asInstanceOf[Output]
    val nclasses = modelOutput.nclasses()
    val classNames = modelOutput.classNames()
    val pred = m.score0(row, new Array[Double](nclasses + 1))
    val predProb = pred slice (1, pred.length)
    val maxProbIdx = ArrayUtils.maxIndex(predProb)
    (classNames(maxProbIdx), predProb)
  }
}
