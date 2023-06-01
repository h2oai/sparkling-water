package ai.h2o.sparkling.ml.utils

import java.io.File

import hex.genmodel.{ModelMojoReader, MojoModel, MojoReaderBackendFactory}
import org.apache.spark.expose.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

object Utils extends Logging {
  def getMojoModel(mojoFile: File): MojoModel = {
    try {
      val reader = MojoReaderBackendFactory.createReaderBackend(mojoFile.getAbsolutePath)
      ModelMojoReader.readFrom(reader, true)
    } catch {
      case e: Throwable =>
        logError(s"Reading a mojo model with metadata failed. Trying to load the model without metadata...", e)
        val reader = MojoReaderBackendFactory.createReaderBackend(mojoFile.getAbsolutePath)
        ModelMojoReader.readFrom(reader, false)
    }
  }

  def arrayToRow[T](array: Array[T]): Row = new GenericRow(array.map(_.asInstanceOf[Any]))
}
