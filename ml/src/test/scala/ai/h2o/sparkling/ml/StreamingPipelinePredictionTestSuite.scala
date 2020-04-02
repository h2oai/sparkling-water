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

package ai.h2o.sparkling.ml

import java.io.{File, PrintWriter}
import java.nio.file.Files

import ai.h2o.sparkling.TestUtils
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StreamingPipelinePredictionTestSuite extends PipelinePredictionTestBase {

  test("Test streaming pipeline with H2O MOJO") {
    //
    val model_path = getClass.getResource("/sms_pipeline.model")
    val pipelineModel = PipelineModel.read.load(model_path.getFile)

    //
    // Define input data
    //
    val smsDataFileName = "smsData.txt"
    val smsDataFilePath = TestUtils.locate(s"smalldata/$smsDataFileName")
    sc.addFile(smsDataFilePath)

    val tmpPath = Files.createTempDirectory(s"SparklingWater-${getClass.getSimpleName}").toAbsolutePath
    val tmpDir = tmpPath.toFile
    tmpDir.setWritable(true, false)
    tmpDir.setReadable(true, false)
    tmpDir.setExecutable(true, false)
    tmpDir.deleteOnExit()

    val data = load(sc, "smsData.txt")
    // Create data for streaming input
    data.select("text").collect().zipWithIndex.foreach {
      case (r, idx) =>
        val printer = new PrintWriter(new File(tmpDir, s"$idx.txt"))
        printer.write(r.getString(0))
        printer.close()
    }
    val schema = StructType(Seq(StructField("text", StringType)))
    val inputDataStream = spark.readStream.schema(schema).text(tmpDir.getAbsolutePath)

    val outputDataStream = pipelineModel.transform(inputDataStream)
    outputDataStream.writeStream.format("memory").queryName("predictions").start()

    //
    // Run predictions on the loaded model which was trained in PySparkling pipeline defined
    // py/examples/pipelines/ham_or_spam_multi_algo.py
    //
    var predictions1 = spark.sql("select * from predictions")

    while (predictions1.count() != 1324) { // The file has 380 entries
      Thread.sleep(1000)
      predictions1 = spark.sql("select * from predictions")
    }

    //
    // UNTIL NOW, RUNTIME WAS NOT AVAILABLE
    //
    // Run predictions on the trained model right now in Scala
    val predictions2 = trainedPipelineModel(spark).transform(data).drop("label")

    TestUtils.assertDataFramesAreIdentical(predictions1, predictions2)
  }

}
