import java.net.URI
import ai.h2o.sparkling._
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import spark.implicits._


val hc = H2OContext.getOrCreate()
