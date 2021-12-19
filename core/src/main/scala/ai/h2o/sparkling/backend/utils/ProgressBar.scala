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

package ai.h2o.sparkling.backend.utils

object ProgressBar {
  private final val BarLength = 50
  private final val CarriageReturn = "\r"
  private final val Empty = "_"
  private final val Filled = "█"
  private final val Side = "|"

  private[sparkling] def printProgressBar(progress: Float, leaveTheProgressBarVisible: Boolean = false): Unit = {
    val crOrNewline = if (leaveTheProgressBarVisible) System.lineSeparator else CarriageReturn
    print(CarriageReturn + renderProgressBar(progress) + crOrNewline)
  }

  private[sparkling] def renderProgressBar(progress: Float): String = {
    val filledPartLength = (progress * BarLength).ceil.toInt
    val filledPart = Filled * filledPartLength
    val emptyPart = Empty * (BarLength - filledPartLength)
    val progressPercent = (progress * 100).toInt
    val percentagePart = s" $progressPercent%"
    Side + filledPart + emptyPart + Side + percentagePart
  }

}
