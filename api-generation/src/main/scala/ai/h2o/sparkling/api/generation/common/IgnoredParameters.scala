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
  def deprecated(algorithm: String): Seq[String] = algorithm match {
    case "H2OGAM" => Seq("r2_stopping", "lambda_min_ratio")
    case "H2OGLRM" => Seq("r2_stopping", "loading_name")
    case "H2ODeepLearning" => Seq("r2_stopping", "col_major", "max_confusion_matrix_size")
    case _ => Seq("r2_stopping")
  }

  val implementedInParent: Seq[String] = Seq("training_frame", "validation_frame")

  def all(algorithm: String): Seq[String] = implementedInParent ++ deprecated(algorithm) ++ {
    algorithm match {
      case "H2OGAM" =>
        Seq(
          "plug_values", // According to MK the parameter doesn't make much sense for GAM
          "interaction_pairs") // Interaction pairs are not currently supported on MOJO
      case "H2ODeepLearning" => Seq("pretrained_autoencoder", "checkpoint")
      case "H2OGBM" => Seq("checkpoint")
      case "H2ODRF" => Seq("checkpoint")
      case "H2OXGBoost" => Seq("checkpoint")
      case "H2OWord2Vec" => Seq("pre_trained")
      case _ => Seq.empty
    }
  }

  def ignoredInMOJOs(algorithm: String): Seq[String] = {
    val common = manuallyImplementedInMOJOModel ++ unimplementedForMOJOs
    common ++ {
      algorithm match {
        case "H2OGAMMOJOModel" => Seq("seed") // Not propagated correctly!
        case "H2OPCAMOJOModel" => Seq("pca_impl") // Not propagated correctly!
        case _ => Seq.empty
      }
    }
  }

  val manuallyImplementedInMOJOModel: Seq[String] = Seq("ntrees", "offset_column")

  val unimplementedForMOJOs: Seq[String] = Seq("model_id", "parallelize_cross_validation")
}
