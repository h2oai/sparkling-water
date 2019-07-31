package ai.h2o.sparkling.ml.models

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

private[models] trait HasMojoData {

  // Called during init of the model
   def setMojoData(mojoData : Array[Byte]): this.type = {
    this.mojoData = mojoData
    broadcastMojo = SparkSession.builder().getOrCreate().sparkContext.broadcast(this.mojoData)
    this
  }

  protected def getMojoData(): Array[Byte] = broadcastMojo.value

  @transient private var mojoData: Array[Byte] = _
  private var broadcastMojo: Broadcast[Array[Byte]] = _
}
