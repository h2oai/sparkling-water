package ai.h2o.sparkling.backend

import ai.h2o.sparkling.SparkTestContext
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers, OptionValues}

@RunWith(classOf[JUnitRunner])
class PartitionStatsGeneratorTestSuite extends FunSuite with SparkTestContext with Matchers with OptionValues {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  private final val dataset =
    Seq((1, "John", "Doe", 1999), (2, "John", "Doe", 1999), (3, "Jane", "Doe", 1999), (4, "Jane", "Doe", 1999))

  private val datasetCols = Seq("id", "name", "surname", "birthYear")

  test("should correctly detect constant columns") {
    val input = dataset.toDF(datasetCols: _*).rdd

    val resultOnConstantColumn = PartitionStatsGenerator.getPartitionStats(input, Some(Seq("surname")))
    val resultOnConstantColumns = PartitionStatsGenerator.getPartitionStats(input, Some(Seq("surname", "birthYear")))
    val resultOnNotConstantColumn = PartitionStatsGenerator.getPartitionStats(input, Some(Seq("name")))
    val resultOnNotConstantColumns = PartitionStatsGenerator.getPartitionStats(input, Some(Seq("name", "id")))
    val resultWhereOnlyOneColumnIsConstant =
      PartitionStatsGenerator.getPartitionStats(input, Some(Seq("surname", "id")))

    resultOnConstantColumn.areFeatureColumnsConstant.value shouldBe true
    resultOnConstantColumns.areFeatureColumnsConstant.value shouldBe true
    resultOnNotConstantColumn.areFeatureColumnsConstant.value shouldBe false
    resultOnNotConstantColumns.areFeatureColumnsConstant.value shouldBe false
    resultWhereOnlyOneColumnIsConstant.areFeatureColumnsConstant.value shouldBe false
  }

  test("should correctly count values") {
    val inputWithTwoPartitions = dataset.toDF(datasetCols: _*).rdd.coalesce(numPartitions = 2)

    val result = PartitionStatsGenerator.getPartitionStats(inputWithTwoPartitions, Some(Seq("id")))

    result.areFeatureColumnsConstant.value shouldBe false
    result.partitionSizes should have size 2
    result.partitionSizes should contain theSameElementsAs Map(0 -> 2, 1 -> 2)
  }

  test("should not fail given an empty dataset") {
    val emptyInput = Seq.empty[String].toDF.rdd

    val result = PartitionStatsGenerator.getPartitionStats(emptyInput, Some(Seq("id")))

    result.areFeatureColumnsConstant shouldBe None
  }

  test("should not fail given one element dataset") {
    val oneElementInput = Seq(dataset.head).toDF(datasetCols: _*).rdd

    val result = PartitionStatsGenerator.getPartitionStats(oneElementInput, Some(Seq("id")))

    result.areFeatureColumnsConstant.value shouldBe true
    result.partitionSizes should have size 1
    result.partitionSizes shouldBe Map(0 -> 1)
  }

}
