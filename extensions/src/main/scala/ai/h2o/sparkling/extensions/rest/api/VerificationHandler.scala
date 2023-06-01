package ai.h2o.sparkling.extensions.rest.api

import ai.h2o.sparkling.extensions.rest.api.schema.VerifyVersionV3.NodeWithVersionV3
import ai.h2o.sparkling.extensions.rest.api.schema.{VerifyVersionV3, VerifyWebOpenV3}
import water.api.Handler
import water.{H2O, MRTask}

import scala.collection.mutable.ArrayBuffer

final class VerificationHandler extends Handler {

  def verifyWebOpen(version: Int, request: VerifyWebOpenV3): VerifyWebOpenV3 = {
    val nodesWithWebDisabled = new VerifyWebOpenMRTask().doAllNodes().nodesWithDisabledWeb
    request.nodes_web_disabled = nodesWithWebDisabled.toArray
    request
  }

  def verifyVersion(version: Int, request: VerifyVersionV3): VerifyVersionV3 = {
    val nodesWithWrongVersion = new VerifyVersionMRTask(request.referenced_version).doAllNodes().nodesWithWrongVersion
    request.nodes_wrong_version = nodesWithWrongVersion.toArray
    request
  }

  class VerifyWebOpenMRTask extends MRTask[VerifyWebOpenMRTask] {
    val nodesWithDisabledWeb: ArrayBuffer[String] = ArrayBuffer.empty

    override def setupLocal(): Unit = {
      if (H2O.ARGS.disable_web) {
        nodesWithDisabledWeb += H2O.SELF.getIpPortString
      }
    }

    override def reduce(mrt: VerifyWebOpenMRTask): Unit = {
      nodesWithDisabledWeb.appendAll(mrt.nodesWithDisabledWeb)
    }
  }

  class VerifyVersionMRTask(referencedVersion: String) extends MRTask[VerifyVersionMRTask] {
    var nodesWithWrongVersion: ArrayBuffer[NodeWithVersionV3] = ArrayBuffer.empty

    override def setupLocal(): Unit = {
      val currentVersion = H2O.ABV.projectVersion
      if (referencedVersion != currentVersion) {
        val nodeWithVersion = new NodeWithVersionV3
        nodeWithVersion.ip_port = H2O.getIpPortString
        nodeWithVersion.version = currentVersion
        nodesWithWrongVersion += nodeWithVersion
      }
    }

    override def reduce(mrt: VerifyVersionMRTask): Unit = {
      mrt.nodesWithWrongVersion.appendAll(mrt.nodesWithWrongVersion)
    }
  }
}
