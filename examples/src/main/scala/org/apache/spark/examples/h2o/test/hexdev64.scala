// Start H2O
import org.apache.spark.h2o._
import org.apache.spark.examples.h2o._
val h2oContext = new H2OContext(sc).start()
import h2oContext._


// Import all year airlines into H2O
val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
val time = new water.util.Timer
val d = new java.net.URI(path)
val airlinesData = new DataFrame(d)
val timeToParse = time.time/(1000*60)
println("Time it took to parse 116 million airlines = " + timeToParse + "mins")

// Transfer data from H2O to Spark RDD
import org.apache.spark.sql.SQLContext
val time = new water.util.Timer
implicit val sqlContext = new SQLContext(sc)
val airlinesRDD =  asSchemaRDD(airlinesData) (sqlContext)
val timeToTransfer = time.time
println("Time it took to convert data to SparkRDD = " + timeToTransfer + "mins")

if(airlinesData.numRows == airlinesRDD.count) {println("Transfer of H2ORDD to SparkRDD completed correctly!")} else {System.exit(1)}
