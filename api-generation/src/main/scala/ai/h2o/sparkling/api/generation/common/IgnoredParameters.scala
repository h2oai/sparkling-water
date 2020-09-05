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

object IgnoredParameters {
  val deprecated: Seq[String] = Seq(
    "r2_stopping", // All
    "max_confusion_matrix_size", // Deep Learning
    "col_major", // Deep Learning
    "max_hit_ratio_k", // GBM, DRF, Deep Learning
    "loading_name", // GLRM
    "lambda_min_ratio") // GAM

  val implementedInParent: Seq[String] = Seq("training_frame", "validation_frame")

  val unimplemented = Seq(
    "__meta", // just for internal purposes
    "checkpoint", // GBM, DRF, XGBoost, Deep Learning
    "interaction_pairs") // GLM, GAM

  val unsupervisedAlgos = Seq("response_column", "offset_column")

  def common: Seq[String] = deprecated ++ implementedInParent ++ unimplemented

  def all(algorithm: String): Seq[String] = common ++ {
    algorithm match {
      case "H2OKMeans" =>
        Seq(
          "response_column",
          "offset_column",
          "distribution",
          "tweedie_power",
          "quantile_alpha",
          "huber_alpha",
          "gainslift_bins",
          "stopping_rounds",
          "stopping_metric",
          "stopping_tolerance",
          "custom_metric_func",
          "custom_distribution_func")
      case "H2OGAM" => Seq("plug_values") // According to MK the parameter doesn't make much sense for GAM
      case "H2ODeepLearning" => Seq("pretrained_autoencoder")
      case _ => Seq.empty
    }
  }
}
