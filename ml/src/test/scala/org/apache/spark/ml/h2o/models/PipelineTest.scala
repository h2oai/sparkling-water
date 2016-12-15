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
package org.apache.spark.ml.h2o.models

import hex.tree.gbm.GBMModel
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkFiles}
import org.scalatest.FunSuite
import water.support.SparkContextSupport

class PipelineTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local",
    conf = defaultSparkConf)

  test("Basic pipeline test") {
    val h2oContext = H2OContext.getOrCreate(sc)
    val inp = sc.parallelize(Seq(
      Row(Vectors.dense(1987, 10, 14, 3, 741, 730, 912, 849, 1451, 91, 79, 23, 11, 447, 0, 0), 1),
      Row(Vectors.dense(1987, 10, 15, 4, 729, 730, 903, 849, 1451, 94, 79, 14, -1, 447, 0, 0), 0),
      Row(Vectors.dense(1987, 10, 17, 6, 741, 730, 918, 849, 1451, 97, 79, 29, 11, 447, 0, 0), 1),
      Row(Vectors.dense(1987, 10, 18, 7, 729, 730, 847, 849, 1451, 78, 79, -2, -1, 447, 0, 0), 1),
      Row(Vectors.dense(1987, 10, 19, 1, 749, 730, 922, 849, 1451, 93, 79, 33, 19, 447, 0, 0), 0),
      Row(Vectors.dense(1987, 10, 21, 3, 728, 730, 848, 849, 1451, 80, 79, -1, -2, 447, 0, 0), 1),
      Row(Vectors.dense(1987, 10, 22, 4, 728, 730, 852, 849, 1451, 84, 79, 3, -2, 447, 0, 0), 1)
    ))

    val inpDF = sqlContext.createDataFrame(
      inp,
      StructType(Seq(
        StructField("features", SQLDataTypes.VectorType),
        StructField("label", DataTypes.IntegerType)
      ))
    )

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val airlinesData = h2oContext.asH2OFrame(inpDF)

    airlinesData.replace(airlinesData.numCols() - 1, airlinesData.lastVec().toCategoricalVec)
    airlinesData.update()

    val gbmParams = new GBMModel.GBMParameters()
    gbmParams._min_rows = 3
    val h2oDL = new H2OGBM(gbmParams)(hc, sqlContext)
      .setLabelCol("label")
      .setFeaturesCol("normFeatures")

    val pipeline = new Pipeline().setStages(Array(normalizer, h2oDL))

    val model = pipeline.fit(inpDF)
    val pred = model.transform(inpDF)

    val toSet: Set[Seq[Any]] = pred.collect().toSet.map((row: Row) => row.toSeq)
    assert(
      toSet == Set(
        Seq(0.8874898801690766, 1987, 10, 14, 3, 741, 730, 912, 849, 1451, 91, 79, 23, 11, 447, 0, 0, 1, 0.27041371801850844,
          0.0013609145345672292, 0.001905280348394121, 4.0827436037016874E-4, 0.10084376701143168, 0.09934676102340773,
          0.1241154055525313, 0.11554164398475776, 0.19746869896570496, 0.012384322264561785, 0.01075122482308111,
          0.003130103429504627, 0.0014970059880239522, 0.06083287969515514, 0, 0),
        Seq(0.250582088450236, 1987, 10, 15, 4, 729, 730, 903, 849, 1451, 94, 79, 14, -1, 447, 0, 0, 0, 0.2717079174073568,
          0.0013674278681799535, 0.0020511418022699304, 5.469711472719814E-4, 0.09968549159031861, 0.09982223437713661,
          0.1234787364966498, 0.11609462600847806, 0.19841378367291126, 0.012853821960891562, 0.010802680158621633,
          0.0019143990154519349, -1.3674278681799536E-4, 0.06112402570764392, 0, 0),
        Seq(0.6774014264478215, 1987, 10, 17, 6, 741, 730, 918, 849, 1451, 97, 79, 29, 11, 447, 0, 0, 1, 0.26953336950623985,
          0.0013564839934888768, 0.0023060227889310906, 8.138903960933261E-4, 0.10051546391752578, 0.09902333152468801,
          0.1245252306022789, 0.11516549104720564, 0.19682582745523602, 0.013157894736842105, 0.010716223548562127,
          0.003933803581117743, 0.0014921323928377645, 0.060634834508952795, 0, 0),
        Seq(1.0363657886295448, 1987, 10, 18, 7, 729, 730, 847, 849, 1451, 78, 79, -2, -1, 447, 0, 0, 1, 0.2746371803731859,
          0.00138217000691085, 0.00248790601243953, 9.675190048375951E-4, 0.10076019350380097, 0.10089841050449205,
          0.117069799585349, 0.11734623358673117, 0.20055286800276434, 0.01078092605390463, 0.010919143054595716,
          -2.7643400138217003E-4, -1.3821700069108501E-4, 0.061782999308914996, 0, 0),
        Seq(0.26061078367222634, 1987, 10, 19, 1, 749, 730, 922, 849, 1451, 93, 79, 33, 19, 447, 0, 0, 0, 0.2689132494248207,
          0.0013533631073216945, 0.002571389903911219, 1.3533631073216944E-4, 0.10136689673839491, 0.0987955068344837,
          0.12478007849506023, 0.11490052781161185, 0.19637298687237786, 0.012586276898091758, 0.010691568547841385,
          0.004466098254161592, 0.002571389903911219, 0.060495330897279743, 0, 0),
        Seq(0.7887639805807599, 1987, 10, 21, 3, 728, 730, 848, 849, 1451, 80, 79, -1, -2, 447, 0, 0, 1, 0.2745992260917634,
          0.0013819789939192924, 0.002902155887230514, 4.1459369817578774E-4, 0.10060807075732449, 0.10088446655610835,
          0.11719181868435599, 0.11733001658374792, 0.20052515201768933, 0.01105583195135434, 0.01091763405196241,
          -1.3819789939192924E-4, -2.763957987838585E-4, 0.061774461028192375, 0, 0),
        Seq(1.0987859049013684, 1987, 10, 22, 4, 728, 730, 852, 849, 1451, 84, 79, 3, -2, 447, 0, 0, 1, 0.274144591611479,
          0.0013796909492273732, 0.0030353200883002206, 5.518763796909492E-4, 0.10044150110375276, 0.10071743929359823,
          0.11754966887417219, 0.11713576158940397, 0.20019315673289184, 0.011589403973509934, 0.010899558498896247,
          4.139072847682119E-4, -2.759381898454746E-4, 0.06167218543046358, 0, 0)
      )
    )
  }

  test("More complex pipeline test") {
    val smsDataFileName = "smsData.txt"
    val smsDataFilePath = "examples/smalldata/" + smsDataFileName

    SparkContextSupport.addFiles(sc, smsDataFilePath)

    def load(dataFile: String)(implicit sqlContext: SQLContext): DataFrame = {
      val smsSchema = StructType(Array(
        StructField("label", StringType, nullable = false),
        StructField("text", StringType, nullable = false)))
      val rowRDD = sc.textFile(SparkFiles.get(dataFile)).map(_.split("\t")).filter(r => !r(0).isEmpty).map(p => Row(p(0), p(1)))
      sqlContext.createDataFrame(rowRDD, smsSchema)
    }

    implicit val sqlContext = SQLContext.getOrCreate(sc)
    import org.apache.spark.h2o._
    implicit val h2oContext = H2OContext.getOrCreate(sc)

    val tokenizer = new RegexTokenizer().
      setInputCol("text").
      setOutputCol("words").
      setMinTokenLength(3).
      setGaps(false).
      setPattern("[a-zA-Z]+")

    val stopWordsRemover = new StopWordsRemover().
      setInputCol(tokenizer.getOutputCol).
      setOutputCol("filtered").
      setStopWords(Array("the", "a", "", "in", "on", "at", "as", "not", "for")).
      setCaseSensitive(false)

    val hashingTF = new HashingTF().
      setNumFeatures(1 << 10).
      setInputCol(tokenizer.getOutputCol).
      setOutputCol("wordToIndex")

    val idf = new IDF().
      setMinDocFreq(4).
      setInputCol(hashingTF.getOutputCol).
      setOutputCol("tf_idf")

    val dl = new H2OGBM().
      setFeaturesCol("tf_idf").
      setLabelCol("label")

    val pipeline = new Pipeline().
      setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, dl))

    val data = load("smsData.txt")
    val model = pipeline.fit(data)

    val smsSchema = StructType(Array(StructField("text", StringType, nullable = false)))

    val inp = Array(
      "I dunno until when... Lets go learn pilates...",
      "Someonone you know is trying to contact you via our dating service! To find out who it could be call from your mobile or landline " +
      "09064015307BOX334SK38ch",
      "Ok c � then.",
      "URGENT! We are trying to contact U. Todays draw shows that you have won a �800 prize GUARANTEED. Call 09050003091 from land line. " +
        "Claim C52. Valid12hrs only"
    )

    val rowRDD = sc.parallelize(inp).map(p => Row(p))
    val test = sqlContext.createDataFrame(rowRDD, smsSchema)

    val result = model.transform(test).collect().map(row => Array(row.getAs[String]("predict"), row.getAs[String]("text")).mkString(" ") ).array

    val labeledInp = Array("ham", "spam", "ham", "spam").zip(inp).map{ case (v1, v2) => v1 + " " + v2 }

    assert( result.sorted sameElements labeledInp.sorted )

    def isSpam(smsText: String,
               model: PipelineModel,
               h2oContext: H2OContext,
               hamThreshold: Double = 0.5): Boolean = {
      import h2oContext.implicits._
      val smsTextSchema = StructType(Array(StructField("text", StringType, nullable = false)))
      val smsTextRowRDD = sc.parallelize(Seq(smsText)).map(Row(_))
      val smsTextDF = sqlContext.createDataFrame(smsTextRowRDD, smsTextSchema)
      val prediction: H2OFrame = model.transform(smsTextDF)
      prediction.vecs()(1).at(0) < hamThreshold
    }
  }

}
