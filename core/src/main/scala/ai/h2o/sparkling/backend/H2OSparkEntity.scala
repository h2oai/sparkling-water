package ai.h2o.sparkling.backend

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.extensions.serde.ExpectedTypes.ExpectedType
import org.apache.spark.Partition

/**
  * Contains functions that are shared between H2O DataFrames and RDDs.
  */
private[backend] trait H2OSparkEntity {
  val frame: H2OFrame
  val selectedColumnIndices: Array[Int]
  val expectedTypes: Array[ExpectedType]

  val frameId: String = frame.frameId
  val numChunks: Int = frame.chunks.length

  protected def getPartitions: Array[Partition] = {
    val res = new Array[Partition](numChunks)
    for (i <- 0 until numChunks) {
      res(i) = new Partition {
        val index: Int = i
      }
    }
    res
  }

  /** Base implementation for iterator over rows stored in H2O chunks for given partition. */
  trait H2OChunkIterator[+A] extends Iterator[A] {

    val reader: Reader

    override def hasNext: Boolean = reader.hasNext
  }

}
