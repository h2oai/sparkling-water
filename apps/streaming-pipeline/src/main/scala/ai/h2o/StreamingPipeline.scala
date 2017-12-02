package ai.h2o

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object StreamingPipeline {

    def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().master("local").getOrCreate()

      import org.apache.spark.sql.types.DataType
      val pipelineModel = PipelineModel.read.load("py/examples/pipelines/Pipeline.model")

      val schema = StructType(DataType.fromJson(scala.io.Source.fromFile("py/examples/pipelines/schema.json").mkString).asInstanceOf[StructType].map {
        case StructField(name, dtype, nullable, metadata) => StructField(name, dtype, true, metadata)
        case rec => rec
      })
      // Print schema
      println(schema)

      val inputDataStream = spark.readStream.schema(schema).csv("/tmp/input/*.csv")

      val outputDataStream = pipelineModel.transform(inputDataStream)

      outputDataStream.writeStream.format("memory").queryName("predictions").start()

      while(true){
        spark.sql("select * from predictions").show()
        Thread.sleep(5000)
      }
    }
}