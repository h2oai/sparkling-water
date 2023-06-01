package ai.h2o.sparkling.api.generation.common

object MetricFieldExceptions {
  def ignored(): Set[String] =
    Set("__meta", "domain", "model", "model_checksum", "frame", "frame_checksum", "model_category", "predictions")

  def optional(): Set[String] = Set("custom_metric_name", "custom_metric_value", "mean_score", "mean_normalized_score")
}
