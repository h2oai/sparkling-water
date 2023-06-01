package ai.h2o.sparkling.backend

import org.apache.spark.SparkContext

import scala.reflect.ClassTag

private[backend] abstract class H2OAwareEmptyRDD[U: ClassTag](sc: SparkContext, nodes: Array[NodeDesc])
  extends H2OAwareBaseRDD[U](sc, nodes)
