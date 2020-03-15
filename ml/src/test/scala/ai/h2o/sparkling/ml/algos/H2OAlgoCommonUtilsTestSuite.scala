package ai.h2o.sparkling.ml.algos

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.{FunSuite, Matchers}

class H2OAlgoCommonUtilsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

  val datasetSchema = (new StructType)
    .add("preds.probability", "int", true)
    .add("39_ClusterDist6:PAY_0.9", "double", true)
    .add("35_TruncSVD:AGE:BILL_AMT3:BILL_AMT4:PAY_3:PAY_6:PAY_AMT4.0", "double", false)

  class DummyTestClass(override val uid: String) extends Transformer with H2OAlgoCommonUtils {

    override def transform(dataset: Dataset[_]): DataFrame = ???

    override def copy(extra: ParamMap): Transformer = ???

    override def transformSchema(schema: StructType): StructType = ???

    override protected def getExcludedCols(): Seq[String] = Nil

    def exposedTestMethod = prepareDatasetForFitting _
  }

  test("Columns sanitation: DAI type of columns names") {
    // Given
    val dataset = spark.createDataFrame(
      sc.parallelize(1 to 5, 5).map(i => Row(i, 2.0*i, i.toDouble)),
      datasetSchema)

    val utils = new DummyTestClass("43")

    // When: transform
    val (trainHf, testHf, internalFeatureCols) = utils.exposedTestMethod(dataset)
    testHf shouldBe None
    internalFeatureCols shouldBe datasetSchema.fields.map(_.name)
  }
}
