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

package ai.h2o.sparkling.api.generation.common

object IgnoredOutputs {
  val ignoredTypes: Set[String] = Set("FrameKeyV3", "FrameKeyV3[]")

  val implementedInParent: Seq[String] = Seq(
    "names",
    "original_names",
    "column_types",
    "domains",
    "cross_validation_models",
    "model_category",
    "scoring_history",
    "training_metrics",
    "validation_metrics",
    "cross_validation_metrics",
    "cross_validation_metrics_summary",
    "cv_scoring_history",
    "reproducibility_information_table",
    "model_summary",
    "start_time",
    "end_time",
    "run_time",
    "default_threshold")

  val ignored: Seq[String] = Seq("status", "help", "__meta")

  def all(mojoModel: String): Seq[String] = implementedInParent ++ ignored ++ {
    mojoModel match {
      case "H2OGLRMMOJOModel" => Seq("representation_name") // Collision with a parameter
      case "H2OWord2VecMOJOModel" => Seq("epochs") // Collision with a parameter
      case _ => Seq.empty
    }
  }
}
