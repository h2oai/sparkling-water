package water.api

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import water.Iced

/**
 * RDDs handler.
 */
class RDDsHandler(val sc: SparkContext) extends Handler {

  def list(version:Int, s: RDDsV3): RDDsV3 = {
    val r = s.createAndFillImpl()
    r.rdds = fetchAll()

    s.fillFromImpl(r)
  }

  def fetchAll():Array[IcedRDDInfo] =
    sc.getPersistentRDDs.values.map(IcedRDDInfo.fromRdd(_)).toArray
}

/** Simple implementation pojo holding list of RDDs */
private[api] class RDDs extends Iced[RDDs] {
  var rdds: Array[IcedRDDInfo]  = _
}

private[api] class IcedRDDInfo(val id: Int,
                               val name: String,
                               val partitions: Int) extends Iced[IcedRDDInfo] {
}


private[api] object IcedRDDInfo {
  def fromRdd(rdd: RDD[_]): IcedRDDInfo = {
    val rddName = Option(rdd.name).getOrElse(rdd.id.toString)
    new IcedRDDInfo(rdd.id, rddName, rdd.partitions.size)
  }
}
