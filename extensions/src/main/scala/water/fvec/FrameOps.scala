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

package water.fvec

import water.Futures

import scala.util.{Failure, Success, Try}

/**
 * High-level DSL proving user-friendly operations
 * on top of H2O Frame.
 */
trait FrameOps {
  self: H2OFrame =>

  /** Functional type to transform vectors. */
  type VecTransformation = (String, Vec) => Vec

  /** Functional type to select vectors. */
  type VecSelector = (String, Vec) => Boolean

  /** Create a sub-frame based on the list of column names.
   *
   * @param columnNames name of columns which will compose a new frame
   * @return a new H2O Frame composed of selected vectors
   */
  def apply(columnNames: Array[String]): H2OFrame = new H2OFrame(subframe(columnNames))

  /** Create a sub-frame based on the list of column names.
   *
   * @param columnNames name of columns which will compose a new frame
   * @return a new H2O Frame composed of selected vectors
   */
  def apply(columnNames: Symbol*): H2OFrame = apply(columnNames.map(_.name).toArray)

  def apply(transformation: VecTransformation, colNames: Symbol*): H2OFrame =
    apply(transformation, colNames.map(_.name).toArray)

  def apply(transformation: VecTransformation, colNames: Array[String]): H2OFrame = {
    apply(transformation, (name, _) => colNames.contains(name))
  }

  def apply(transformation: VecTransformation, selector: VecSelector, removeVec: Boolean = true): H2OFrame = {
    val futures = new Futures()
    self.names().zipWithIndex.foreach { case (name, idx) =>
      val vec = self.vec(name)
      if (selector(name, vec)) {
        val newVec = transformation(name, vec)
        val oldVec = self.replace(idx, newVec)
        if (removeVec) {
          oldVec.remove(futures)
        }
      }
    }
    // Block for all fired deletes
    futures.blockForPending()
    self
  }

  /**
   * Transform columns in enum columns
   *
   * @param cols : Array[ String ] containing all the names of enum columns
   */
  def colToEnum(cols: Array[String]): Unit = {
    if (cols.diff(this.names()).isEmpty) {
      val colIndices = this.find(cols)
      colIndices.foreach(idx => this.replace(idx, this.vec(idx).toCategoricalVec))
      this.update()
    } else {
      throw new IllegalArgumentException("One or several columns are not present in your DataFrame")
    }
  }

  /**
   * Transform columns in enum columns
   *
   * @param cols : Array[ Int ] containing all the indexes of enum columns
   */
  def colToEnum(cols: Array[Int]): Unit = {
    Try(cols.map(i => this.name(i))) match {
      case Success(s) => colToEnum(s)
      case Failure(t) => println(t)
    }
  }

  /**
   * Rename a column of your DataFrame
   *
   * @param index   : Index of the column to rename
   * @param newName : New name
   */
  def rename(index: Int, newName: String): Unit = {
    val tmp = this.names
    Try(tmp(index) = newName) match {
      case Success(_) => this._names = tmp
      case Failure(t) => println(t)
    }
  }

  /**
   * Rename a column of your DataFrame
   *
   * @param oldName : Old name
   * @param newName : New name
   */
  def rename(oldName: String, newName: String): Unit = {
    val index = this.find(oldName)
    if (index != -1) {
      rename(index, newName)
    } else {
      throw new IllegalArgumentException(s"Column '$oldName' is missing")
    }
  }
}
