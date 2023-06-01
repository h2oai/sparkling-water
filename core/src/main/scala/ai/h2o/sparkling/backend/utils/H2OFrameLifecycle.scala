package ai.h2o.sparkling.backend.utils

import ai.h2o.sparkling.H2OFrame

import scala.collection.mutable.ArrayBuffer

trait H2OFrameLifecycle {
  private val h2oFramesToBeDeleted = new ArrayBuffer[H2OFrame]()

  private[sparkling] final def registerH2OFrameForDeletion(frame: H2OFrame): Unit = h2oFramesToBeDeleted.append(frame)

  private[sparkling] final def registerH2OFrameForDeletion(frame: Option[H2OFrame]): Unit = {
    frame.foreach(registerH2OFrameForDeletion)
  }

  private[sparkling] final def deleteRegisteredH2OFrames(): Unit = {
    h2oFramesToBeDeleted.foreach(_.delete())
    h2oFramesToBeDeleted.clear()
  }
}
