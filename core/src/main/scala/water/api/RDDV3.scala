package water.api

import org.apache.spark.rdd.RDD

/**
 * Endpoint representing single RDD.
 */
class RDDV3 extends Schema[IcedRDDInfo, RDDV3] {

  @API(help = "RDD id", direction = API.Direction.OUTPUT)
  val id: Int = 0

  @API(help = "RDD name", direction = API.Direction.OUTPUT)
  val name: String = null

  @API(help = "Number of partitions", direction = API.Direction.OUTPUT)
  val partitions: Int = 0
}
