package ai.h2o

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object StreamingPipeline {

    def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().getOrCreate()

      import org.apache.spark.sql.types.DataType
      val schema = StructType(DataType.fromJson(scala.io.Source.fromFile("/Users/kuba/devel/repos/h2o/sparkling-water/py/examples/pipelines/schema.json").mkString).asInstanceOf[StructType])

      // Print schema
      println(schema)

      val inputDataStream = spark.readStream.schema(schema).csv("/Users/kuba/devel/repos/h2o/sparkling-water/py/examples/pipelines/Reviews.csv")
      val pipelineModel = PipelineModel.read.load("/Users/kuba/devel/repos/h2o/sparkling-water/py/examples/pipelines/Pipeline.model")

      val outputDataStream = pipelineModel.transform(inputDataStream)

      outputDataStream.writeStream.format("memory").queryName("results").start()

      while(true){
        spark.sql("select * from results").show()
        Thread.sleep(5000)
      }
    }
}