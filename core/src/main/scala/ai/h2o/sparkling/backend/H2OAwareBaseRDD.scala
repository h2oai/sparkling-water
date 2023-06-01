package ai.h2o.sparkling.backend

import ai.h2o.sparkling.H2OContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{OneToOneDependency, Partition, SparkContext}

import scala.reflect.ClassTag

private[backend] abstract class H2OAwareBaseRDD[U: ClassTag] private (
    sc: SparkContext,
    nodes: Array[NodeDesc],
    prev: Option[RDD[U]])
  extends RDD[U](sc, Seq(prev.map(new OneToOneDependency(_))).flatten) {

  def this(nodes: Array[NodeDesc], prev: RDD[U]) = this(prev.sparkContext, nodes, Some(prev))

  def this(sc: SparkContext, nodes: Array[NodeDesc]) = this(sc, nodes, None)

  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (H2OContext.ensure().getConf.runsInInternalClusterMode) {
      nodes.map(nodeDesc => s"executor_${nodeDesc.hostname}_${nodeDesc.nodeId}")
    } else {
      super.getPreferredLocations(split)
    }
  }
}
