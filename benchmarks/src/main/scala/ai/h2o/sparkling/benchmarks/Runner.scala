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

import java.io.{File, FileOutputStream, InputStreamReader}
import java.lang.reflect.Modifier

import com.google.common.reflect.ClassPath
import org.apache.spark.SparkConf
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.Serialization._

import scala.collection.JavaConverters._

object Runner {
  val defaultDatasetSpecificationsFile = "datasets.json"
  val defaultOutputDir = new File("benchmarks", "output")

  val spark = SparkSession
    .builder()
    .config(createSparkConf())
    .getOrCreate()

  val hc = H2OContext.getOrCreate()

  def createSparkConf(): SparkConf = {
    val conf = new SparkConf()

    // If master is not defined in system properties or environment variables, fallback to local.
    val master = conf.get("spark.master", "local[*]")
    conf.setMaster(master)

    // If the application name is not defined in system properties or environment variables,
    // set it to the class name.
    val appName = conf.get("spark.app.name", this.getClass.getSimpleName)
    conf.setAppName(appName)

    conf
  }

  def main(args: Array[String]): Unit = {
    val settings = processArguments(args)
    val datasetDetails = loadDatasetDetails(settings.datasetSpecificationsFile)
    val benchmarks = getBenchmarkClasses()
    val algorithms = AlgorithmBenchmarkBase.supportedAlgorithms

    val filteredDatasetDetails = filterCollection[DatasetDetails]("Dataset", settings.dataset, datasetDetails, _.name)
    val filteredBenchmarks = filterCollection[Class[_]]("Benchmark", settings.benchmark, benchmarks, _.getSimpleName)
    val filteredAlgorithms = filterCollection[AlgorithmBundle](
      "Algorithm",
      settings.algorithm,
      algorithms,
      _.h2oAlgorithm.getClass.getSimpleName)

    val outputDir = settings.outputDir match {
      case Some(dir) => new File(dir)
      case None => defaultOutputDir
    }

    val batches = createBatches(filteredDatasetDetails, filteredBenchmarks, filteredAlgorithms)
    batches.foreach(batch => executeBatch(batch, outputDir))

    hc.stop(stopSparkContext = true)
  }

  private def processArguments(args: Array[String]): Settings = {
    require(
      args.length % 2 == 0,
      "Wrong arguments. Example: -s datasetSpecificationFile -b benchmarkName -d datasetName -a algorithmName -o outputDir")
    val (keys, values) = args.zipWithIndex.partition { case (_, idx) => idx % 2 == 0 }
    val map = keys.map(_._1).zip(values.map(_._1)).toMap
    Settings(
      map.getOrElse("-s", defaultDatasetSpecificationsFile),
      map.get("-b"),
      map.get("-d"),
      map.get("-a"),
      map.get("-o"))
  }

  private def loadDatasetDetails(datasetSpecificationsFile: String): Seq[DatasetDetails] = {
    val stream = getClass.getClassLoader.getResourceAsStream(datasetSpecificationsFile)
    val reader = new InputStreamReader(stream)
    implicit val formats = DefaultFormats
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
      val inheritsFromBenchmarkBase = classOf[BenchmarkBase[_]].isAssignableFrom(clazz)
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
      require(result.nonEmpty, s"$entity '$name' does not exist!")
      result
  }

  private def createBatches(
      datasetDetails: Seq[DatasetDetails],
      benchmarkClasses: Seq[Class[_]],
      algorithms: Seq[AlgorithmBundle]): Seq[BenchmarkBatch] = {
    def isAlgorithmBenchmark(clazz: Class[_]): Boolean = classOf[AlgorithmBenchmarkBase[_]].isAssignableFrom(clazz)

    val benchmarkContexts = datasetDetails.map(BenchmarkContext(spark, hc, _))
    benchmarkClasses.map { benchmarkClass =>
      val parameterSets = if (isAlgorithmBenchmark(benchmarkClass)) {
        for (context <- benchmarkContexts; algorithm <- algorithms) yield Array(context, algorithm.newInstance())
      } else {
        benchmarkContexts.map(Array(_))
      }
      val benchmarkInstances = parameterSets.map { parameterSet =>
        benchmarkClass.getConstructors()(0).newInstance(parameterSet: _*).asInstanceOf[BenchmarkBase[_]]
      }
      BenchmarkBatch(benchmarkClass.getSimpleName, benchmarkInstances)
    }
  }

  private def executeBatch(batch: BenchmarkBatch, outputDir: File) = {
    println(s"Executing benchmark batch '${batch.name}' ...")
    batch.benchmarks.foreach { benchmark =>
      benchmark.run()
      benchmark.exportMeasurements(System.out)
      new DKVCleaner().clean()
    }
    outputDir.mkdirs()
    val sparkMaster = spark.conf.get("spark.master")
    val outputFile = new File(outputDir, s"${sparkMaster}_${hc.getConf.backendClusterMode}_${batch.name}.txt")
    val outputStream = new FileOutputStream(outputFile)
    try {
      batch.benchmarks.foreach(_.exportMeasurements(outputStream))
    } finally {
      outputStream.close()
    }
    println(s"Benchmark batch '${batch.name}' has finished.")
  }

  private case class BenchmarkBatch(name: String, benchmarks: Seq[BenchmarkBase[_]])

  private case class Settings(
      datasetSpecificationsFile: String,
      benchmark: Option[String],
      dataset: Option[String],
      algorithm: Option[String],
      outputDir: Option[String])

}
