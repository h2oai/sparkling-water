package ai.h2o.sparkling.backend.api.rdds

import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest
import org.apache.spark.rdd.RDD

/** Schema representing /3/RDDs/[rdd_id] endpoint */
case class RDDInfo(rdd_id: Int, name: String, partitions: Int)

object RDDInfo extends ParameterBase with RDDCommons {
  def fromRDD(rdd: RDD[_]): RDDInfo = {
    new RDDInfo(rdd.id, Option(rdd.name).getOrElse(rdd.id.toString), rdd.partitions.length)
  }

  private[api] case class RDDInfoParameters(rddId: Int) {
    def validate(): Unit = validateRDDId(rddId)
  }

  private[api] object RDDInfoParameters {
    def parse(request: HttpServletRequest): RDDInfoParameters = {
      val rddId = request.getPathInfo.drop(1).split("/").head.toInt
      RDDInfoParameters(rddId)
    }
  }
}
