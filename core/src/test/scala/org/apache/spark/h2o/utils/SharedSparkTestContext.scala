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
package org.apache.spark.h2o.utils

import org.apache.spark.SparkContext
import org.apache.spark.h2o.{Holder, H2OConf, H2OContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.Suite

import scala.collection.mutable

/**
  * Helper trait to simplify initialization and termination of Spark/H2O contexts.
  *
  */
trait SharedSparkTestContext extends SparkTestContext { self: Suite =>

  def createSparkContext:SparkContext

  def createH2OContext(sc: SparkContext, conf: H2OConf): H2OContext = {
    H2OContext.getOrCreate(sc, conf)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = createSparkContext
    sqlc = SQLContext.getOrCreate(sc)
    hc = createH2OContext(sc, new H2OConf(sc))
  }

  override def afterAll(): Unit = {
    resetContext()
    super.afterAll()
  }
}

class PUBDEV458Type(val result: Option[Int]) extends Holder[Int] with Product with Serializable {
  //def this() = this(None)
  override def canEqual(that: Any): Boolean = that.isInstanceOf[PUBDEV458Type]

  override def productArity: Int = 1

  override def productElement(n: Int) =
    n match {
      case 0 => result
      case _ => throw new IndexOutOfBoundsException(n.toString)
    }

  override def toString = s"PUBDEV458Type($result)"
}

class TestMemory[T] extends scala.collection.mutable.HashSet[T] with mutable.SynchronizedSet[T] {
  def put(xh: Holder[T]): Unit = xh.result foreach put

  def put(x: T): Unit = {
    if (this contains x) {
      throw new IllegalStateException(s"Duplicate element $x in test memory")
    }
    add(x)
  }
}

