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

package ai.h2o.sparkling.benchmarks

import java.io.{File, FileOutputStream, FileReader}
import java.lang.reflect.Modifier

import ai.h2o.sparkling.ml.algos.H2OAlgorithm
import com.google.common.reflect.ClassPath
import org.apache.spark.SparkConf
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.Serialization._

import scala.collection.JavaConverters._

object Runner {
  val datasetDetailsFilePath = "datasets.json"
  val outputDir = new File("benchmarks", "output")

  val spark = SparkSession
    .builder()
    .config(createSparkConf())
    .getOrCreate()

  new H2OConf(spark).setExternalClusterMode()

  val hc = H2OContext.getOrCreate(spark)

  def createSparkConf(): SparkConf = {
    val conf = new SparkConf()

    // If master is not defined in system properties or environment variables, fallback to local.
    val master = conf.get("spark.master", "local")
    conf.setMaster(master)

    // If the application name is not defined in system properties or environment variables,
    // set it to the class name.
    val appName = conf.get("spark.app.name", this.getClass.getSimpleName)
    conf.setAppName(appName)

    conf
  }

  def main(args: Array[String]): Unit = {
    val settings = processArguments(args)
    val datasetDetails = loadDatasetDetails()
    val benchmarks = getBenchmarkClasses()
    val algorithms = AlgorithmBenchmarkBase.supportedAlgorithms

    val filteredDatasetDetails = filterCollection[DatasetDetails]("Dataset", settings.dataset, datasetDetails, _.name)
    val filteredBenchmarks = filterCollection[Class[_]]("Benchmark", settings.benchmark, benchmarks, _.getSimpleName)
    val filteredAlgorithms = filterCollection[H2OAlgorithm[_, _, _]]("Algorithm", settings.algorithm, algorithms, _.getClass.getSimpleName)

    val batches = createBatches(filteredDatasetDetails, filteredBenchmarks, filteredAlgorithms)
    batches.foreach(executeBatch)

    hc.stop(stopSparkContext = true)
  }

  private def processArguments(args: Array[String]): Settings = {
    require(args.length % 2 == 0, "Wrong arguments. Example: -b benchmarkName -d datasetName -a algorithmName")
    val (keys, values) = args.zipWithIndex.partition { case (_, idx) => idx % 2 == 1 }
    val map = keys.map(_._1).zip(values.map(_._1)).toMap
    Settings(map.get("-b"), map.get("-d"), map.get("-a"))
  }

  private def loadDatasetDetails(): Seq[DatasetDetails] = {
    val url = getClass.getClassLoader.getResource(datasetDetailsFilePath)
    implicit val formats = DefaultFormats
    val reader = new FileReader(url.getPath)
    try {
      read[Seq[DatasetDetails]](reader)
    } finally {
      reader.close()
    }
  }

  private def getBenchmarkClasses(): Seq[Class[_]] = {
    val classLoader = Thread.currentThread().getContextClassLoader
    val classPath = ClassPath.from(classLoader)
    val packageName = this.getClass.getPackage.getName

    def isBenchmark(clazz: Class[_]) = {
      val isAbstract = Modifier.isAbstract(clazz.getModifiers)
      val inheritsFromBenchmarkBase  = classOf[BenchmarkBase].isAssignableFrom(clazz)
      !isAbstract && inheritsFromBenchmarkBase
    }

    val classes = classPath.getTopLevelClasses(packageName).asScala.map(_.load())
    classes.filter(isBenchmark(_)).toSeq
  }

  private def filterCollection[T](
      entity: String,
      filter: Option[String],
      collection: Seq[T],
      nameGetter: T => String): Seq[T] = filter match {
    case None => collection
    case Some(name) =>
      val result = collection.filter(nameGetter(_) == name)
      require(result.length > 0, s"$entity '$name' does not exist!")
      result
  }

  private def createBatches(
      datasetDetails: Seq[DatasetDetails],
      benchmarkClasses: Seq[Class[_]],
      algorithms: Seq[H2OAlgorithm[_, _, _]]): Seq[BenchmarkBatch]  = {
    def isAlgorithmBenchmark(clazz: Class[_]): Boolean = classOf[AlgorithmBenchmarkBase].isAssignableFrom(clazz)
    val benchmarkContexts = datasetDetails.map(BenchmarkContext(spark, hc, _))
    benchmarkClasses.map { benchmarkClass =>
      val parameterSets = if (isAlgorithmBenchmark(benchmarkClass)) {
        for (context <- benchmarkContexts; algorithm <- algorithms) yield Array(context, algorithm.copy(ParamMap.empty))
      } else {
        benchmarkContexts.map(Array(_))
      }
      val benchmarkInstances = parameterSets.map { parameterSet =>
        benchmarkClass.getConstructors()(0).newInstance(parameterSet : _*).asInstanceOf[BenchmarkBase]
      }
      BenchmarkBatch(benchmarkClass.getSimpleName, benchmarkInstances)
    }
  }

  private def executeBatch(batch: BenchmarkBatch) = {
    println(s"Executing benchmark batch '${batch.name}' ...")
    batch.benchmarks.foreach { benchmark =>
      benchmark.run()
      benchmark.exportMeasurements(System.out)
    }
    outputDir.mkdirs()
    val outputFile = new File(outputDir, s"${hc._conf.backendClusterMode}_${batch.name}.txt")
    val outputStream = new FileOutputStream(outputFile)
    try {
      batch.benchmarks.foreach(_.exportMeasurements(outputStream))
    } finally {
      outputStream.close()
    }
    println(s"Benchmark batch '${batch.name}' has finished.")
  }

  private case class BenchmarkBatch(name: String, benchmarks: Seq[BenchmarkBase])

  private case class Settings(benchmark: Option[String], dataset: Option[String], algorithm: Option[String])
}
