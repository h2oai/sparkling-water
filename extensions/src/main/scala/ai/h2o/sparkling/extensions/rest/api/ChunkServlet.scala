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

import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import ai.h2o.sparkling.utils.ScalaUtils._
import ai.h2o.sparkling.utils.Base64Encoding
import ai.h2o.sparkling.extensions.serde.{ChunkAutoBufferReader, ChunkAutoBufferWriter, ChunkSerdeConstants}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import water.DKV
import water.fvec.Frame
import water.server.ServletUtils

/**
  * This servlet class handles GET and PUT requests for the path /3/Chunk
  */
final class ChunkServlet extends HttpServlet {

  private case class GETRequestParameters(
                                           frameName: String,
                                           numRows: Int,
                                           chunkId: Int,
                                           expectedTypes: Array[Byte],
                                           selectedColumnIndices: Array[Int]) {

    def validate(): Unit = {
      val frame = DKV.getGet[Frame](this.frameName)
      if (frame == null) throw new RuntimeException(s"A frame with name '$frameName' doesn't exist.")
      validateChunkId(frame)
      validateSelectedColumns(frame)
      validateExpectedTypes(expectedTypes, frame)
      validateExpectedTypesAndSelectedColumnsCompatibility(frame)
    }

    def validateChunkId(frame: Frame): Unit = {
      if (chunkId < 0) {
        throw new RuntimeException(s"chunk_id can't be negative. Current value: $chunkId")
      }
      val numberOfChunks = frame.anyVec.nChunks
      if (chunkId >= numberOfChunks) {
        val message = s"chunk_id '$chunkId' is out of range." +
          s"The frame '$frameName' has $numberOfChunks chunks."
        throw new RuntimeException(message)
      }
    }

    def validateSelectedColumns(frame: Frame): Unit = {
      for (i <- selectedColumnIndices.indices) {
        if (selectedColumnIndices(i) < 0) {
          val message = s"Selected column index ('selected_columns') at position $i " +
            s"with the value '${selectedColumnIndices(i)}' is negative."
          throw new RuntimeException(message)
        }
        if (selectedColumnIndices(i) >= frame.numCols) {
          val message = s"Selected column index ('selected_columns') at position $i " +
            s"with the value '${selectedColumnIndices(i)}' is out of range. " +
            s"Frame '$frameName' has ${frame.numCols} columns."
          throw new RuntimeException(message)
        }
      }
    }

    def validateExpectedTypesAndSelectedColumnsCompatibility(frame: Frame): Unit = {
      if (selectedColumnIndices.length != expectedTypes.length) {
        val message = s"The number of expected_types '${expectedTypes.length}' is not the same as " +
          s"the number of selected_columns '${selectedColumnIndices.length}'"
        throw new RuntimeException(message)
      }
    }
  }

  private object GETRequestParameters {
    def parse(request: HttpServletRequest): GETRequestParameters = {
      val frameName = getParameterAsString(request, "frame_name")
      val numRowsString = getParameterAsString(request, "num_rows")
      val numRows = numRowsString.toInt
      val chunkIdString = getParameterAsString(request, "chunk_id")
      val chunkId = chunkIdString.toInt
      val expectedTypesString = getParameterAsString(request, "expected_types")
      val expectedTypes = Base64Encoding.decode(expectedTypesString)
      val selectedColumnsString = getParameterAsString(request, "selected_columns")
      val selectedColumnIndices = Base64Encoding.decodeToIntArray(selectedColumnsString)
      GETRequestParameters(frameName, numRows, chunkId, expectedTypes, selectedColumnIndices)
    }
  }

  def validateExpectedTypes(expectedTypes: Array[Byte], frame: Frame): Unit = {
    val lowerBound = ChunkSerdeConstants.EXPECTED_BOOL
    val upperBound = ChunkSerdeConstants.EXPECTED_VECTOR
    for (i <- expectedTypes.indices) {
      if (expectedTypes(i) < lowerBound || expectedTypes(i) > upperBound) {
        val message = s"Expected Type ('expected_types') at position $i with " +
          s"the value '${expectedTypes(i)}' is invalid."
        throw new RuntimeException(message)
      }
    }
  }

  private def getParameterAsString(request: HttpServletRequest, parameterName: String): String = {
    val result = request.getParameter(parameterName)
    if (result == null) {
      throw new RuntimeException(s"Cannot find value for the parameter '$parameterName'")
    }
    result
  }

  private def processRequest[R](request: HttpServletRequest, response: HttpServletResponse)(processor: => Unit): Unit = {
    val uri = ServletUtils.getDecodedUri(request)
    try {
      processor
      ServletUtils.setResponseStatus(response, HttpServletResponse.SC_OK)
    } catch {
      case e: Exception => ServletUtils.sendErrorResponse(response, e, uri)
    } finally {
      ServletUtils.logRequest(request.getMethod, request, response)
    }
  }

  /*
   * The method handles handles GET requests for the path /3/Chunk
   * It requires 4 get parameters
   * - frame_name - a unique string identifier of H2O Frame
   * - chunk_id - a unique identifier of the chunk within the H2O Frame
   * - expected_type - byte array encoded in Base64 encoding. The types corresponds to the `selected_columns` parameter
   * - selected_columns - selected columns indices encoded into Base64 encoding.
   * The result is represented as a stream of binary data. Data are encoded to AutoBuffer row by row.
   * The data stream starts with the integer representing the number of rows.
   */
  override protected def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    processRequest(request, response) {
      val parameters = GETRequestParameters.parse(request)
      parameters.validate()
      response.setContentType("application/octet-stream")
      withResource(response.getOutputStream) { outputStream =>
        withResource(new GZIPOutputStream(outputStream)) { compressed =>
          withResource(new ChunkAutoBufferWriter(compressed)) { writer =>
            writer.writeChunk(
              parameters.frameName,
              parameters.numRows,
              parameters.chunkId,
              parameters.expectedTypes,
              parameters.selectedColumnIndices)
          }
        }
      }
    }
  }

  private case class PUTRequestParameters(
                                           frameName: String,
                                           numRows: Int,
                                           chunkId: Int,
                                           expectedTypes: Array[Byte],
                                           maxVecSizes: Array[Int]) {
    def validate(): Unit = {
      val frame = DKV.getGet[Frame](this.frameName)
      if (frame == null) throw new RuntimeException(s"A frame with name '$frameName")
      validateExpectedTypes(expectedTypes, frame)
      validateMaxVecSizes()
    }

    def validateMaxVecSizes(): Unit = {
      val numberOfVectorTypes = expectedTypes.filter(_ == ChunkSerdeConstants.EXPECTED_VECTOR).length
      if (numberOfVectorTypes != maxVecSizes.length) {
        val message = s"The number of vector types ($numberOfVectorTypes) doesn't correspond to" +
          s"the number of items in 'maximum_vector_sizes' (${maxVecSizes.length})"
        new RuntimeException(message)
      }
    }
  }

  private object PUTRequestParameters {
    def parse(request: HttpServletRequest): PUTRequestParameters = {
      val frameName = getParameterAsString(request, "frame_name")
      val numRowsString = getParameterAsString(request, "num_rows")
      val numRows = numRowsString.toInt
      val chunkIdString = getParameterAsString(request, "chunk_id")
      val chunkId = chunkIdString.toInt
      val expectedTypesString = getParameterAsString(request, "expected_types")
      val expectedTypes = Base64Encoding.decode(expectedTypesString)
      val maximumVectorSizesString = getParameterAsString(request, "maximum_vector_sizes")
      val maxVecSizes = Base64Encoding.decodeToIntArray(maximumVectorSizesString)
      PUTRequestParameters(frameName, numRows, chunkId, expectedTypes, maxVecSizes)
    }
  }

  /*
   * The method handles handles PUT requests for the path /3/Chunk
   * It requires 4 get parameters
   * - frame_name - a unique string identifier of H2O Frame
   * - chunk_id - a unique identifier of the chunk within the H2O Frame
   * - expected_type - byte array encoded in Base64 encoding. The types corresponds to the `selected_columns` parameter
   * - maximum_vector_sizes - maximum vector sizes for each vector column encoded into Base64 encoding.
   * The result is represented as a stream of binary data. Data are encoded to AutoBuffer row by row.
   * The data stream starts with the integer representing the number of rows.
   */
  override def doPut(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    processRequest(request, response) {
      val parameters = PUTRequestParameters.parse(request)
      parameters.validate()
      withResource(request.getInputStream) { inputStream =>
        withResource(new GZIPInputStream(inputStream)) { decompressed =>
          withResource(new ChunkAutoBufferReader(decompressed)) { reader =>
            reader.readChunk(
              parameters.frameName,
              parameters.numRows,
              parameters.chunkId,
              parameters.expectedTypes,
              parameters.maxVecSizes
            )
          }
        }
        ServletUtils.setResponseStatus(response, HttpServletResponse.SC_OK)
      }
    }
  }
}
