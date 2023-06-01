package ai.h2o.sparkling.extensions.rest.api

import ai.h2o.sparkling.extensions.internals.LocalNodeDomains
import ai.h2o.sparkling.utils.Compression
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import water.{AutoBuffer, DKV, Key}
import water.fvec.Frame
import water.server.ServletUtils

/**
  * This servlet class handles PUT requests for the path /3/ChunkCategoricalDomains
  */
class ChunkCategoricalDomainsServlet extends ServletBase {
  private case class PUTRequestParameters(frameName: String, chunkId: Int, compression: String) {
    def validate(): Unit = {
      val frame = DKV.getGet[Frame](this.frameName)
      if (frame == null) throw new IllegalArgumentException(s"A frame with name '$frameName' does not exist.")
      Compression.validateCompressionType(compression)
    }
  }

  private object PUTRequestParameters {
    def parse(request: HttpServletRequest): PUTRequestParameters = {
      val frameName = getParameterAsString(request, "frame_name")
      val chunkIdString = getParameterAsString(request, "chunk_id")
      val chunkId = chunkIdString.toInt
      val compression = getParameterAsString(request, "compression")
      PUTRequestParameters(frameName, chunkId, compression)
    }
  }

  /**
    * The method handles handles PUT requests for the path /3/ChunkCategoricalDomains
    * It requires 3 get parameters
    * - frame_name - a unique string identifier of H2O Frame
    * - chunk_id - a unique identifier of the chunk within the H2O Frame
    * - compression - a type of compression applied on the content
    */
  override def doPut(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    processRequest(request, response) {
      val parameters = PUTRequestParameters.parse(request)
      parameters.validate()
      withResource(request.getInputStream) { inputStream =>
        withResource(Compression.decompress(parameters.compression, inputStream)) { decompressed =>
          val autoBuffer = new AutoBuffer(decompressed)
          val domains = autoBuffer.getAAStr()
          val frameKey = Key.make(parameters.frameName)
          LocalNodeDomains.addDomains(frameKey, parameters.chunkId, domains)
          autoBuffer.close()
        }
      }
      ServletUtils.setResponseStatus(response, HttpServletResponse.SC_OK)
    }
  }
}
