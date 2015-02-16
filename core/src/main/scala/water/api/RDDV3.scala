package water.api

import org.apache.spark.rdd.RDD

/**
 * Endpoint representing single RDD.
 */
class RDDV3 extends Schema[IcedRDDInfo, RDDV3] {

  @API(help = "RDD id", direction = API.Direction.OUTPUT)
  var id: Int = _

  @API(help = "RDD name", direction = API.Direction.OUTPUT)
  var name: String = _

  @API(help = "Number of partitions", direction = API.Direction.OUTPUT)
  var partitions: Int = _
}
