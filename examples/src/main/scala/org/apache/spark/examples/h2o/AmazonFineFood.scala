/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.examples.h2o

import org.apache.spark.SparkConf
import org.apache.spark.h2o._
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.{DateTimeZone, MutableDateTime}
import water.MRTask
import water.fvec._
import water.support.{ModelMetricsSupport, SparkContextSupport, SparkSessionSupport}

object AmazonFineFood extends SparkContextSupport with SparkSessionSupport with ModelMetricsSupport {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = configure("Amazon Fine Food Review Sentiment Analysis")
    val sc = sparkContext(conf)

    @transient val hc = H2OContext.getOrCreate(sc)

    val reviews = new H2OFrame(new java.io.File("/Users/michal/Tmp/amazon-fine-foods/Reviews.csv"))

    // We do not need redundant data
    reviews.remove("Id").remove
    reviews.remove("ProfileName").remove

    // Update in DKV
    reviews.update


    val refinedTime = new RefineTime().doAll(5, Vec.T_NUM, reviews).outputFrame(Array("Day", "Month", "Year", "DayOfWeek", "Hour"), null /* no domains */)
    reviews.add(refinedTime).remove("Time").remove()
    reviews.update
    // NOTE: hour is not useful

    val df = hc.asDataFrame(reviews)(sqlContext)
    df.printSchema()

    import org.apache.spark.sql.functions._
    val avgScorePerYear = hc.asH2OFrame(df.groupBy("Year").agg(mean("Score"), count("Score")), "avgScorePerYear")
    val avgScorePerMonth = hc.asH2OFrame(df.groupBy("Month").agg(mean("Score"), count("Score")), "avgScorePerMonth")
    val avgScorePerDay = hc.asH2OFrame(df.groupBy("DayOfWeek").agg(mean("Score"), count("Score")), "avgScorePerDay")

    // Input for sentiment analysis
    val sentimentDF = hc.asDataFrame(reviews('Score, 'Month, 'Day, 'DayOfWeek, 'Summary))(sqlContext)

    // Transform Score to binary +/- feature - skip neutral reviews
    val toBinaryScore = udf { score: Byte => if (score < 3.toByte) "negative" else "positive" }

    val toTokens = udf { summary: String =>
      summary.split(",")
        .map(v => v.trim.toLowerCase.replaceAll("[^\\p{IsAlphabetic}]", ""))
          .filter(v => !H2OStopWords.English.contains(v))
    }

    val hashingTF = new HashingTF(4096) // Larger space?
    val toNumericFeatures = udf { terms: Seq[_] => hashingTF.transform(terms) }

    // Skip all neutral reviews
    val vectorizedFrame: DataFrame = sentimentDF.where("Score != 3")
      .withColumn("Score", toBinaryScore(col("Score")))
      .withColumn("Summary", toNumericFeatures(toTokens(col("Summary"))))

    val idfModel = new IDF(minDocFreq = 1).fit(vectorizedFrame.select("Summary").rdd.map { case Row(v: org.apache.spark.mllib.linalg.Vector) => v})
    val toIdf = udf { vector: org.apache.spark.mllib.linalg.Vector => idfModel.transform(vector)}
    val finalFrame: DataFrame = vectorizedFrame.withColumn("Summary", toIdf(col("Summary")))
    finalFrame.printSchema()

    val p = hc.asH2OFrame(finalFrame, "finalFrame")
    // Cleanup
    reviews.delete()

    // RUN GLM or SVM

    // Create a predictor function
    println("DONE")
  }
}

class RefineTime extends MRTask[RefineTime] {
  override def map(in: Array[Chunk], out: Array[NewChunk]): Unit = {
    val mdt = new MutableDateTime(DateTimeZone.UTC)
    val timeCol = in(5 /* Index of Time column*/)
    val (dayNC, monthNC, yearNC, dayOfWeekNC, hourNC) = (out(0), out(1), out(2), out(3), out(4))
    for (row <- 0 until timeCol._len) {
      val time = timeCol.at8(row) * 1000 /* JODA API expect millis seconds from epoch */
      mdt.setMillis(time)
      dayNC.addNum(mdt.getDayOfMonth, 0)
      monthNC.addNum(mdt.getMonthOfYear, 0)
      yearNC.addNum(mdt.getYear, 0)
      dayOfWeekNC.addNum(mdt.getDayOfWeek, 0)
      hourNC.addNum(mdt.getHourOfDay, 0)
    }
  }
}

object H2OStopWords {
  val English = StopWordsRemover.loadDefaultStopWords("english")
}

/*
plot (g) -> g(
  g.point(
    g.position "Year", "avg(Score)"
    g.fillColor "count(Score)"
    g.size "count(Score)"
  )
  g.from inspect "data", getFrame "avgScorePerYear"
)
 */
