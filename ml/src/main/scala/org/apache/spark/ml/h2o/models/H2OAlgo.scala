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

import java.io._

import hex.Model
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import water.Key
import water.fvec.Frame

import scala.reflect.ClassTag
/**
  * Base class for H2O algorithm wrapper as a Spark transformer.
  */
abstract class H2OAlgorithm[P <: Model.Parameters : ClassTag,
                            M <: H2OModel[M, _ <: Model[_, P, _]] : ClassTag]
                            (parameters: Option[P])
                            (implicit h2oContext: H2OContext, sqlContext: SQLContext)
  extends Estimator[M] with MLWritable with H2OParams[P] {

  type SELF

  if(parameters.isDefined){
    setParams(parameters.get)
  }

  override def fit(dataset: Dataset[_]): M = {
    import h2oContext.implicits._
    // check if trainKey is explicitly set
    val key = if(isSet(trainKey)){
      $(trainKey)
    } else {
      h2oContext.toH2OFrameKey(dataset.toDF())
    }
    setTrainKey(key)
    allStringVecToCategorical(key.get())
    // Train
    val model: M = trainModel(getParams)
    model
  }

  def trainModel(params: P): M

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  @Since("1.6.0")
  override def write: MLWriter = new H2OAlgorithmWriter(this)

  /** @group setParam */
  def setValidKey(value: String) = set(validKey,Key.make[Frame](value)){getParams._valid = Key.make[Frame](value)}
  /** @group setParam */
  def setValidKey(value: Key[Frame]) = set(validKey,value){getParams._valid = value}
  /** @group setParam */
  def setTrainKey(value: String) = set(trainKey,Key.make[Frame](value)){getParams._train = Key.make[Frame](value)}
  /** @group setParam */
  def setTrainKey(value: Key[Frame]) = set(trainKey,value){getParams._train = value}

  def allStringVecToCategorical(hf: H2OFrame): H2OFrame = {
    hf.vecs().indices
      .filter(idx => hf.vec(idx).isString)
      .foreach(idx => hf.replace(idx, hf.vec(idx).toCategoricalVec).remove())
    // Update frame in DKV
    water.DKV.put(hf)
    // Return it
    hf
  }

  /**
    * Set the param and execute custom piece of code
    */
  protected final def set[T](param: Param[T], value: T)(f:  => Unit): SELF = {
    f
    super.set(param, value).asInstanceOf[SELF]
  }

  def defaultFileName: String
}

// FIXME: H2O Params are iced objects!
private[models] class H2OAlgorithmWriter[T <: H2OAlgorithm[_, _]](instance: T) extends MLWriter {

  @Since("1.6.0") override protected
  def saveImpl(path: String): Unit = {
    val hadoopConf = sc.hadoopConfiguration
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    val outputPath = if (path.startsWith("file://")) {
      new Path(path, instance.defaultFileName)
    } else {
      new Path("file://" + path, instance.defaultFileName)
    }
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    fs.create(qualifiedOutputPath)
    val oos = new ObjectOutputStream(new FileOutputStream(new File(qualifiedOutputPath.toUri), false))
    oos.writeObject(instance.getParams)
  }
}

private[models] class H2OAlgorithmReader[A <: H2OAlgorithm[P, _] : ClassTag, P <: Model.Parameters : ClassTag]
                                (val defaultFileName: String) extends MLReader[A] {

  private val className = implicitly[ClassTag[A]].runtimeClass.getName

  override def load(path: String): A = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
    val file = new File(path, defaultFileName)
    val ois = new ObjectInputStream(new FileInputStream(file))
    val parameters = ois.readObject().asInstanceOf[P]
    implicit val h2oContext = H2OContext.ensure("H2OContext has to be started in order to use H2O pipelines elements")
    val h2oAlgo = make[A, P](parameters, metadata.uid, h2oContext, sqlContext)
    DefaultParamsReader.getAndSetParams(h2oAlgo, metadata)
    h2oAlgo
  }

  private def make[CT : ClassTag, X <: Object : ClassTag]
                  (p: X, uid: String, h2oContext: H2OContext, sqlContext: SQLContext): CT = {
    val pClass = implicitly[ClassTag[X]].runtimeClass
    val aClass = implicitly[ClassTag[CT]].runtimeClass
    val ctor = aClass.getConstructor(pClass, classOf[String], classOf[H2OContext], classOf[SQLContext])
    ctor.newInstance(p, uid, h2oContext, sqlContext).asInstanceOf[CT]
  }
}


