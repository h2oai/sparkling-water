package ai.h2o.sparkling.api.generation.common

object AutoMLIgnoredParameters {

  def all: Seq[String] =
    Seq(
      "__meta",
      "job",
      "training_frame",
      "validation_frame",
      "blending_frame",
      "leaderboard_frame",
      "monotone_constraints",
      "preprocessing",
      "stopping_criteria",
      "modeling_plan",
      "algo_parameters")
}
