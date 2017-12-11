package ai.h2o

import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object StreamingPipeline {

    def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().master("local").getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      //
      // Load exported pipeline
      //
      import org.apache.spark.sql.types.DataType
      val pipelineModel = PipelineModel.read.load("py/examples/pipelines/reviews_pipeline.model/")

      //
      // Load exported schema of input data
      //
      val schema = StructType(DataType.fromJson(scala.io.Source.fromFile("py/examples/pipelines/schema.json").mkString).asInstanceOf[StructType].map {
        case StructField(name, dtype, nullable, metadata) => StructField(name, dtype, true, metadata)
        case rec => rec
      })
      println(schema)

      //
      // Define input stream
      //
      val inputDataStream = spark.readStream.schema(schema).csv("py/examples/data/kuba/input/*.csv")

      //
      // Apply loaded model
      //
      val outputDataStream = pipelineModel.transform(inputDataStream)

      //
      // Forward output stream into memory-sink
      //
      outputDataStream.writeStream.format("memory").queryName("predictions").start()

      //
      // Query results
      //
      while(true){
        spark.sql("select * from predictions").show()
        Thread.sleep(5000)
      }
    }
}
