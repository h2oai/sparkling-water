package ai.h2o.sparkling.backend

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

private[backend] class H2OAwareRDD[U: ClassTag](nodes: Array[NodeDesc], prev: RDD[U])
  extends H2OAwareBaseRDD[U](nodes, prev) {

  override def compute(split: Partition, context: TaskContext): Iterator[U] = prev.compute(split, context)

  override protected def getPartitions: Array[Partition] = prev.partitions
}
