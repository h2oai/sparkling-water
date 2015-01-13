package org.apache.spark.h2o

import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.H2OContextUtils.NodeDesc

/** Special kind of RDD which is used to invoke code on all executors detected in cluster. */
private[h2o]
class InvokeOnNodesRDD(nodes:Seq[NodeDesc], sc: SparkContext) extends RDD[NodeDesc](sc, Nil) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(String, String, Int)] = {
    Iterator.single(split.asInstanceOf[PartitionWithNodeInfo].nodeDesc)
  }

  override protected def getPartitions: Array[Partition] = nodes.zip(0 until nodes.length ).map( n =>
    new PartitionWithNodeInfo(n._1) {
      override def index: Int = n._2
    }).toArray


  override protected def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[PartitionWithNodeInfo].nodeDesc._2)

  abstract class PartitionWithNodeInfo(val nodeDesc: NodeDesc) extends Partition
}
