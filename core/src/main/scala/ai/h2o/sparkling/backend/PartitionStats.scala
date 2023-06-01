package ai.h2o.sparkling.backend

case class PartitionStats(partitionSizes: Map[Int, Int], areFeatureColumnsConstant: Option[Boolean])
