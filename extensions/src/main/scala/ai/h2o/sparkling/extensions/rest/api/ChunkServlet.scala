/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package ai.h2o.sparkling.extensions.rest.api

import java.nio.{ByteBuffer, ByteOrder}

import ai.h2o.sparkling.utils.ScalaUtils._
import ai.h2o.sparkling.utils.Base64Encoding
import ai.h2o.sparkling.extensions.serde.{SerializationUtils, ChunkAutoBufferWriter}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import water.DKV
import water.fvec.Frame
import water.server.ServletUtils

/**
  * This servlet class handles GET requests for the path /3/Chunk
  * It requires 4 get parameters
  * - frame_name - a unique string identifier of H2O Frame
  * - chunk_id - a unique identifier of the chunk within the H2O Frame
  * - expected_type - byte array encoded in Base64 encoding. The types corresponds to the `selected_columns` parameter
  * - selected_columns - selected columns indices encoded int Base64 encoding.
  * The result is represented as a stream of binary data. Data are encoded to AutoBuffer row by row.
  * The data stream starts with the integer representing the number of rows.
  */
final class ChunkServlet extends HttpServlet {

  private case class RequestParameters(
      frameName: String,
      chunkId: Int,
      expectedTypes: Array[Byte],
      selectedColumnIndices: Array[Int])

  private def getParameterAsString(request: HttpServletRequest, parameterName: String): String = {
    val result = request.getParameter(parameterName)
    if (result == null) {
      throw new RuntimeException(s"Cannot find value for the parameter '$parameterName'")
    }
    result
  }

  private def parseRequestParameters(request: HttpServletRequest): RequestParameters = {
    val frameName = getParameterAsString(request, "frame_name")
    val chunkIdString = getParameterAsString(request, "chunk_id")
    val chunkId = chunkIdString.toInt
    val expectedTypesString = getParameterAsString(request, "expected_types")
    val expectedTypes = Base64Encoding.decode(expectedTypesString)
    val selectedColumnsString = getParameterAsString(request, "selected_columns")
    val selectedColumnIndices = Base64Encoding.decodeToIntArray(selectedColumnsString)
    RequestParameters(frameName, chunkId, expectedTypes, selectedColumnIndices)
  }

  private def validateRequestParameters(parameters: RequestParameters): Unit = {
    val frame = DKV.getGet(parameters.frameName)
    if (frame == null) throw new RuntimeException(s"A frame with name '${parameters.frameName}")
    validateChunkId(parameters, frame)
    validateSelectedColumns(parameters, frame)
    validateExpectedTypes(parameters, frame)
  }

  private def validateChunkId(parameters: RequestParameters, frame: Frame): Unit = {
    if (parameters.chunkId < 0) {
      throw new RuntimeException(s"chunk_id can't be negative. Current value: ${parameters.chunkId}")
    }
    val numberOfChunks = frame.anyVec.nChunks
    if (parameters.chunkId >= numberOfChunks) {
      val message = s"chunk_id '${parameters.chunkId}' is out of range." +
        s"The frame '${parameters.frameName}' has $numberOfChunks chunks."
      throw new RuntimeException(message)
    }
  }

  private def validateSelectedColumns(parameters: RequestParameters, frame: Frame): Unit = {
    for (i <- parameters.selectedColumnIndices.indices) {
      if (parameters.selectedColumnIndices(i) < 0) {
        val message = s"Selected column index ('selected_columns') at position $i " +
          s"with the value '${parameters.selectedColumnIndices(i)}' is negative."
        throw new RuntimeException(message)
      }
      if (parameters.selectedColumnIndices(i) >= frame.numCols) {
        val message = s"Selected column index ('selected_columns') at position $i " +
          s"with the value '${parameters.selectedColumnIndices(i)}' is out of range. " +
          s"Frame '${parameters.frameName}' has ${frame.numCols} columns."
        throw new RuntimeException(message)
      }
    }
  }

  private def validateExpectedTypes(parameters: RequestParameters, frame: Frame): Unit = {
    val lowerBound = SerializationUtils.EXPECTED_BOOL
    val upperBound = SerializationUtils.EXPECTED_VECTOR
    for (i <- parameters.expectedTypes.indices) {
      if (parameters.expectedTypes(i) < lowerBound || parameters.expectedTypes(i) > upperBound) {
        val message = s"Expected Type ('expected_types') at position $i with " +
          s"the value '${parameters.expectedTypes(i)}' is invalid."
        throw new RuntimeException(message)
      }
    }
    if (parameters.selectedColumnIndices.length != parameters.expectedTypes.length) {
      val message = s"The number of expected_types '${parameters.expectedTypes.length}' is not the same as " +
        s"the number of selected_columns '${parameters.selectedColumnIndices.length}'"
      throw new RuntimeException(message)
    }
  }

  override protected def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    val uri = ServletUtils.getDecodedUri(request)
    try {
      val parameters = parseRequestParameters(request)
      validateRequestParameters(parameters)
      response.setContentType("application/octet-stream")
      ServletUtils.setResponseStatus(response, HttpServletResponse.SC_OK)
      withResource(response.getOutputStream) { outputStream =>
        withResource(new ChunkAutoBufferWriter(outputStream)) { writer =>
          writer.writeChunk(
            parameters.frameName,
            parameters.chunkId,
            parameters.expectedTypes,
            parameters.selectedColumnIndices)
        }
      }
    } catch {
      case e: Exception => ServletUtils.sendErrorResponse(response, e, uri)
    } finally {
      ServletUtils.logRequest(request.getMethod, request, response)
    }
  }
}
