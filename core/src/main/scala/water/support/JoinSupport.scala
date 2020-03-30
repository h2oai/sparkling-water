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
package water.support

import water.fvec.Frame
import water.munging.JoinMethod
import water.rapids.Rapids

/**
  * Trait which contains methods for specific H2O join and merge operation.
  */
trait JoinSupport {

  private val MERGE_RAPIDS: String = "(merge %s %s %s %s [] [] \"%s\")"

  /**
    * Join H2O frames
    *
    * @param left   left frame
    * @param right  right frame
    * @param allX   all X values
    * @param allY   all Y values
    * @param method joining method
    * @return
    */
  def join[T <: Frame](
      left: T,
      right: T,
      allX: Boolean = false,
      allY: Boolean = false,
      method: JoinMethod = JoinMethod.AUTO): Frame = {
    val rCode = String.format(MERGE_RAPIDS, left._key, right._key, toStr(allX), toStr(allY), method.name)
    val session = new water.rapids.Session()
    val ret = Rapids.exec(rCode, session)
    ret.getFrame
  }

  /**
    * Left join two frames
    *
    * @param left   left frame
    * @param right  right frame
    * @param method joining method
    * @return new frame
    */
  def leftJoin(left: Frame, right: Frame, method: JoinMethod = JoinMethod.AUTO): Frame = {
    join(left, right, allX = true, allY = false, method = method)
  }

  /**
    * Right join two frames
    *
    * @param left   left frame
    * @param right  right frame
    * @param method joining method
    * @return new frame
    */
  def rightJoin(left: Frame, right: Frame, method: JoinMethod = JoinMethod.AUTO): Frame = {
    join(left, right, allX = false, allY = true, method = method)
  }

  /**
    * Inner join two frames
    *
    * @param left   left frame
    * @param right  right frame
    * @param method joining method
    * @return new frame
    */
  def innerJoin(left: Frame, right: Frame, method: JoinMethod = JoinMethod.AUTO): Frame = {
    join(left, right, allX = false, allY = false, method = method)
  }

  /**
    * Outer join two frames
    *
    * @param left   left frame
    * @param right  right frame
    * @param method joining method
    * @return new frame
    */
  def outerJoin(left: Frame, right: Frame, method: JoinMethod = JoinMethod.AUTO): Frame = {
    join(left, right, allX = true, allY = true, method = method)
  }

  private def toStr(b: Boolean): String = if (b) "1" else "0"
}

object JoinSupport extends JoinSupport {}
