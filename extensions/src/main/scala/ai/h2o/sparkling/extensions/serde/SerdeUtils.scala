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

package ai.h2o.sparkling.extensions.serde

import ai.h2o.sparkling.extensions.serde.ExpectedTypes.ExpectedType
import water.fvec.Vec

object SerdeUtils {
  private[sparkling] def expectedTypesToVecTypes(expectedTypes: Array[ExpectedType], vecElemSizes: Array[Int]): Array[Byte] = {
    var vecCount = 0
    expectedTypes.flatMap {
      case ExpectedTypes.Bool | ExpectedTypes.Byte | ExpectedTypes.Char | ExpectedTypes.Short | ExpectedTypes.Int |
           ExpectedTypes.Long | ExpectedTypes.Float | ExpectedTypes.Double =>
        Array(Vec.T_NUM)
      case ExpectedTypes.String => Array(Vec.T_STR)
      case ExpectedTypes.Categorical => Array(Vec.T_CAT)
      case ExpectedTypes.Timestamp => Array(Vec.T_TIME)
      case ExpectedTypes.Vector =>
        val result = Array.fill(vecElemSizes(vecCount))(Vec.T_NUM)
        vecCount += 1
        result
    }
  }
}
