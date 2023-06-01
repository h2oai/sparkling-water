package ai.h2o.sparkling.ml.models

import java.io.{ByteArrayOutputStream, NotSerializableException, ObjectOutputStream}

import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class H2OMOJOModelSparkRuntimeIndependencyTestSuite extends FunSuite with Matchers {
  def createSparkSession(): SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  test("Score with internal MOJO without Spark runtime") {
    val spark = createSparkSession
    val swMojoModel = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("binom_model_prostate.mojo"),
      "binom_model_prostate.mojo")
    spark.stop()

    val h2oMojoModel = swMojoModel.unwrapMojoModel()

    val config = new EasyPredictModelWrapper.Config()
    config.setModel(h2oMojoModel)
    val wrapper = new EasyPredictModelWrapper(config)

    val rowData = new RowData()
    rowData.put("ID", "1")
    rowData.put("AGE", "65")
    rowData.put("RACE", "1")
    rowData.put("DPROS", "2")
    rowData.put("DCAPS", "1")
    rowData.put("PSA", "1.4")
    rowData.put("VOL", "0")
    rowData.put("GLEASON", "6")

    val prediction = wrapper.predictBinomial(rowData)
    prediction.classProbabilities(0) shouldBe >(0.0)
    prediction.classProbabilities(1) shouldBe >(0.0)

    val domainValues = swMojoModel.getDomainValues()
    domainValues.size shouldBe >(0)
    domainValues.get("capsule").get shouldEqual Array("0", "1")
  }

  test("H2OMOJOModel throws exception if serialized in tests") {
    System.setProperty("spark.testing", "true")
    createSparkSession()
    val swMojoModel = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("binom_model_prostate.mojo"),
      "binom_model_prostate.mojo")
    val memoryStream = new ByteArrayOutputStream()
    val writer = new ObjectOutputStream(memoryStream)

    intercept[NotSerializableException] {
      writer.writeObject(swMojoModel)
    }
  }
}
