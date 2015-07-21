//Prepare environment
import org.apache.spark.h2o._
import org.apache.spark.streaming._
import org.apache.spark.examples.h2o.CraigslistJobTitlesApp
import org.apache.spark.examples.h2o.CraigslistJobTitlesApp._

// Create Spark streaming context
@transient val ssc = new StreamingContext(sc, Seconds(10))

// Create H2O Context
@transient val h2oContext = new H2OContext(sc).start()

// Create the application instance
val staticApp = new CraigslistJobTitlesApp()(sc, sqlContext, h2oContext)

// Build models
@transient val models = staticApp.buildModels("examples/smalldata/craigslistJobTitles.csv", "initialModel")
@transient val gbmModel = models._1
val w2vModel = models._2
val modelId = gbmModel._key.toString
val classNames = gbmModel._output.asInstanceOf[hex.Model.Output].classNames()

// Create Spark stream and expose it on port 9999
@transient val jobTitlesStream = ssc.socketTextStream("localhost", 9999)
jobTitlesStream.filter(!_.isEmpty).
    map(jobTitle => (jobTitle, staticApp.classify(jobTitle, modelId, w2vModel))).
    map(pred => "\"" + pred._1 + "\" = " + show(pred._2, classNames)).
    print()

// Start streaming subsystem
ssc.start()
ssc.awaitTermination()

