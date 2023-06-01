package ai.h2o.sparkling.backend

import water.H2ONode
import water.nbhm.NonBlockingHashMap

/** Helper class containing node ID, hostname and port.
  *
  * @param nodeId   In case of external cluster mode the node ID is ID of H2O Node, in the internal cluster mode the ID
  *                 is ID of Spark Executor where corresponding instance is located
  * @param hostname hostname of the node
  * @param port     port of the node
  */
case class NodeDesc(nodeId: String, hostname: String, port: Int) {
  override def productPrefix = ""

  def ipPort(): String = s"$hostname:$port"
}

object NodeDesc {
  def apply(node: H2ONode): NodeDesc = {
    intern(node)
  }

  private def fromH2ONode(node: H2ONode): NodeDesc = {
    val ipPort = node.getIpPortString.split(":")
    NodeDesc(node.index().toString, ipPort(0), Integer.parseInt(ipPort(1)))
  }

  private def intern(node: H2ONode): NodeDesc = {
    var nodeDesc = INTERN_CACHE.get(node)
    if (nodeDesc != null) {
      nodeDesc
    } else {
      nodeDesc = fromH2ONode(node)
      val oldNodeDesc = INTERN_CACHE.putIfAbsent(node, nodeDesc)
      if (oldNodeDesc != null) {
        oldNodeDesc
      } else {
        nodeDesc
      }
    }
  }

  private val INTERN_CACHE = new NonBlockingHashMap[H2ONode, NodeDesc]
}
