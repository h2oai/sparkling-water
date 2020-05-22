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

package org.apache.spark.h2o

import java.util.concurrent.atomic.AtomicReference

import ai.h2o.sparkling.backend.NodeDesc
import ai.h2o.sparkling.backend.converters._
import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.expose.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkContext, h2o}
import water.api.ImportHiveTableHandler.HiveTableImporter
import water.{DKV, Key}

import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class H2OContext private (val hc: ai.h2o.sparkling.H2OContext) {
  self =>

  val sparkSession: SparkSession = SparkSessionUtils.active
  val sparkContext: SparkContext = sparkSession.sparkContext

  def getH2ONodes(): Array[NodeDesc] = hc.getH2ONodes()

  def getConf: H2OConf = new h2o.H2OConf(hc.getConf.sparkConf)

  def downloadH2OLogs(destinationDir: String, logContainer: String): String = {
    hc.downloadH2OLogs(destinationDir, logContainer)
  }

  def importHiveTable(
      database: String = HiveTableImporter.DEFAULT_DATABASE,
      table: String,
      partitions: Array[Array[String]] = null,
      allowMultiFormat: Boolean = false): H2OFrame = {
    new H2OFrame(hc.importHiveTable(database, table, partitions, allowMultiFormat).frameId)
  }

  object implicits extends H2OContextImplicits with Serializable {
    protected override def _h2oContext: H2OContext = self
  }

  override def toString: String = hc.toString

  def openFlow(): Unit = hc.openFlow()

  def stop(stopSparkContext: Boolean = false): Unit = hc.stop(stopSparkContext)

  def flowURL(): String = hc.flowURL()

  def setH2OLogLevel(level: String): Unit = hc.setH2OLogLevel(level)

  def getH2OLogLevel(): String = hc.getH2OLogLevel()

  def h2oLocalClient: String = hc.h2oLocalClient

  def h2oLocalClientIp: String = hc.h2oLocalClientIp

  def h2oLocalClientPort: Int = hc.h2oLocalClientPort

  def asH2OFrameKeyString(rdd: SupportedRDD): String = asH2OFrameKeyString(rdd, None)

  def asH2OFrameKeyString(rdd: SupportedRDD, frameName: String): String = asH2OFrameKeyString(rdd, Some(frameName))

  def asH2OFrameKeyString(rdd: SupportedRDD, frameName: Option[String]): String = hc.asH2OFrame(rdd, frameName).frameId

  def asH2OFrame(rdd: SupportedRDD): H2OFrame = asH2OFrame(rdd, None)

  def asH2OFrame(rdd: SupportedRDD, frameName: String): H2OFrame = asH2OFrame(rdd, Option(frameName))

  def asH2OFrame(rdd: SupportedRDD, frameName: Option[String]): H2OFrame =
    new H2OFrame(hc.asH2OFrame(rdd, frameName).frameId)

  def toH2OFrameKey(rdd: SupportedRDD): Key[_] = toH2OFrameKey(rdd, None)

  def toH2OFrameKey(rdd: SupportedRDD, frameName: String): Key[_] = toH2OFrameKey(rdd, Option(frameName))

  def toH2OFrameKey(rdd: SupportedRDD, frameName: Option[String]): Key[_] = asH2OFrame(rdd, frameName)._key

  def asH2OFrame(df: DataFrame): H2OFrame = asH2OFrame(df, None)

  def asH2OFrame(df: DataFrame, frameName: String): H2OFrame = asH2OFrame(df, Option(frameName))

  def asH2OFrame(df: DataFrame, frameName: Option[String]): H2OFrame =
    new H2OFrame(hc.asH2OFrame(df, frameName).frameId)

  def asH2OFrameKeyString(df: DataFrame): String = asH2OFrameKeyString(df, None)

  def asH2OFrameKeyString(df: DataFrame, frameName: String): String = asH2OFrameKeyString(df, Some(frameName))

  def asH2OFrameKeyString(df: DataFrame, frameName: Option[String]): String = hc.asH2OFrame(df, frameName).frameId

  def toH2OFrameKey(df: DataFrame): Key[Frame] = toH2OFrameKey(df, None)

  def toH2OFrameKey(df: DataFrame, frameName: String): Key[Frame] = toH2OFrameKey(df, Option(frameName))

  def toH2OFrameKey(df: DataFrame, frameName: Option[String]): Key[Frame] = asH2OFrame(df, frameName)._key

  def asH2OFrame(ds: SupportedDataset): H2OFrame = asH2OFrame(ds, None)

  def asH2OFrame(ds: SupportedDataset, frameName: String): H2OFrame = asH2OFrame(ds, Option(frameName))

  def asH2OFrame(ds: SupportedDataset, frameName: Option[String]): H2OFrame =
    new H2OFrame(hc.asH2OFrame(ds, frameName).frameId)

  def toH2OFrameKey(ds: SupportedDataset): Key[Frame] = toH2OFrameKey(ds, None)

  def toH2OFrameKey(ds: SupportedDataset, frameName: Option[String]): Key[Frame] =
    asH2OFrame(ds, frameName)._key

  def toH2OFrameKey(ds: SupportedDataset, frameName: String): Key[Frame] =
    toH2OFrameKey(ds, Option(frameName))

  def asH2OFrameKeyString(ds: SupportedDataset): String = asH2OFrameKeyString(ds, None)

  def asH2OFrameKeyString(ds: SupportedDataset, frameName: String): String = asH2OFrameKeyString(ds, Some(frameName))

  def asH2OFrameKeyString(ds: SupportedDataset, frameName: Option[String]): String = {
    hc.asH2OFrame(ds, frameName).frameId
  }

  def asH2OFrame(s: String): H2OFrame = new H2OFrame(s)

  def asH2OFrame(fr: Frame): H2OFrame = new H2OFrame(fr)

  def asRDD[A <: Product: TypeTag: ClassTag](fr: H2OFrame): RDD[A] = {
    DKV.put(fr)
    hc.asRDD(ai.h2o.sparkling.H2OFrame(fr._key.toString))
  }

  def asRDD[A <: Product: TypeTag: ClassTag] = new {
    def apply[T <: Frame](fr: T): RDD[A] = {
      DKV.put(fr)
      hc.asRDD[A](ai.h2o.sparkling.H2OFrame(fr._key.toString))
    }
  }

  def asRDD[A <: Product: TypeTag: ClassTag](fr: ai.h2o.sparkling.H2OFrame): org.apache.spark.rdd.RDD[A] = {
    SupportedRDDConverter.toRDD[A](hc, fr)
  }

  def asSparkFrame[T <: Frame](fr: T, copyMetadata: Boolean = true): DataFrame = {
    DKV.put(fr)
    SparkDataFrameConverter.toDataFrame(hc, ai.h2o.sparkling.H2OFrame(fr._key.toString), copyMetadata)
  }

  def asSparkFrame(fr: ai.h2o.sparkling.H2OFrame): DataFrame = {
    SparkDataFrameConverter.toDataFrame(hc, fr, copyMetadata = true)
  }

  def asSparkFrame(s: String, copyMetadata: Boolean): DataFrame = {
    val frame = ai.h2o.sparkling.H2OFrame(s)
    SparkDataFrameConverter.toDataFrame(hc, frame, copyMetadata)
  }

  def asSparkFrame(s: String): DataFrame = asSparkFrame(s, copyMetadata = true)
}

object H2OContext extends Logging {

  private val instantiatedContext = new AtomicReference[H2OContext]()

  @DeprecatedMethod("ai.h2o.sparkling.H2OContext.getOrCreate", "3.34")
  def getOrCreate(): H2OContext = {
    val hc = ai.h2o.sparkling.H2OContext.getOrCreate()
    instantiatedContext.set(new H2OContext(hc))
    instantiatedContext.get()
  }

  @DeprecatedMethod("ai.h2o.sparkling.H2OContext.getOrCreate", "3.34")
  def getOrCreate(conf: H2OConf): H2OContext = synchronized {
    val hc = ai.h2o.sparkling.H2OContext.getOrCreate(conf)
    instantiatedContext.set(new H2OContext(hc))
    instantiatedContext.get()
  }

  @DeprecatedMethod("ai.h2o.sparkling.H2OContext.get", "3.34")
  def get(): Option[H2OContext] = Option(instantiatedContext.get())

  @DeprecatedMethod("ai.h2o.sparkling.H2OContext.ensure", "3.34")
  def ensure(onError: => String = "H2OContext has to be running."): H2OContext = {
    Option(instantiatedContext.get()) getOrElse {
      throw new RuntimeException(onError)
    }
  }
}
