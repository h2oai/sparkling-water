# Real Time Pipeline To H2OFrames Using Sparkling Water

This demo will show you how to take streaming data and create a "live" dataframe over some rolling time window. The potential use cases could be using H2O data munging capabilities on a real time distributed dataframe or mini batch training for online ML using H2O [checkpointing models] (http://docs.h2o.ai/h2o/latest-stable/h2o-docs/index.html?#%E2%80%A6%20Building%20Models-Viewing%20Models-Checkpointing%20Models). 

## Requirements

sbt
Spark 2.0
Sparkling Water 2.0.x
Python

## Directions
  1. Build the project `./gradlew clean jar`

  2. Run the demo class `ai.h2o.PipelineDemo` 
    ```bash
    $SPARK_HOME/bin/spark-submit \
      --class ai.h2o.PipelineDemo \
      --master 'local[*]' \
      --driver-memory 2G \
      --packages ai.h2o:sparkling-water-core_2.11:2.0.0 \
      --conf spark.scheduler.minRegisteredResourcesRatio=1 \
      --conf spark.ext.h2o.repl.enabled=False \
      ./build/libs/*.jar \
      9999
     ```
   
  3. You should see some errors `Error connecting to localhost:9999` as Spark Streaming starts to run but this can be fixed by connecting the stream source with:
  ```bash
  python socket_send_spark.py
  ```

 4. Then you should be able to login in to Flow/R/Python and see the live H2OFrame

## Caveats

When Sparkling Water converts the RDD to an H2OFrame, you will not be able to access it during 
this time.  Its good to test and find the best periodicity in generating your H2OFrame. also, it 
is preferable that all the steps for your use case is done in the events.window loop:

```scala
    events.window(Seconds(300), Seconds(10)).foreachRDD(rdd =>
      {
        if (!rdd.isEmpty ) {
          try {
            hf.delete()
          } catch { case e: Exception => println("Initialized frame") }
          hf = hc.asH2OFrame(rdd.toDF(), "demoFrame10s") //the name of the frame
          // perform your munging, score your events with a pretrained model or
          // mini-batch training on checkpointed models, etc
          // make sure your execution finishes within the batch cycle (the
          // second arg in the window)
        }
      }
    )
```


