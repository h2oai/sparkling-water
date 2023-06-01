package ai.h2o.sparkling.ml

import ai.h2o.sparkling.TestUtils
import org.apache.spark.ml.PipelineModel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PipelinePredictionTestSuite extends PipelinePredictionTestBase {

  /**
    * This test is not using H2O runtime since we are testing deployment of the pipeline
    */
  test("Run predictions on Spark pipeline model containing H2O Mojo") {

    //
    // Load exported pipeline
    //
    val model_path = getClass.getResource("/sms_pipeline.model")
    val pipelineModel = PipelineModel.read.load(model_path.getFile)

    //
    // Define input stream
    //
    val smsDataFileName = "smsData.txt"
    val smsDataFilePath = TestUtils.locate(s"smalldata/$smsDataFileName")
    sc.addFile(smsDataFilePath)

    val inputDataStream = load(sc, "smsData.txt")

    //
    // Run predictions on the loaded model which was trained in PySparkling pipeline defined
    // py/examples/pipelines/ham_or_spam_multi_algo.py
    //
    val predictions1 = pipelineModel.transform(inputDataStream)

    //
    // UNTIL NOW, RUNTIME WAS NOT AVAILABLE
    //
    // Run predictions on the trained model right now in Scala
    val predictions2 = trainedPipelineModel(spark).transform(inputDataStream)

    TestUtils.assertDataFramesAreIdentical(predictions1, predictions2)
  }
}
