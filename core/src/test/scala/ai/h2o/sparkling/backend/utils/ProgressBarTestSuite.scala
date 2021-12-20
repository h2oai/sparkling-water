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

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProgressBarTestSuite extends FunSuite with Matchers {

  test("should return a rendered progress bar - 0%") {
    val input = 0f

    val result = ProgressBar.renderProgressBar(progress = input)

    result shouldBe "|__________________________________________________| 0%"
  }

  test("should return a rendered progress bar - 55%") {
    val input = 0.555555f

    val result = ProgressBar.renderProgressBar(progress = input)

    result shouldBe "|████████████████████████████______________________| 55%"
  }

  test("should return a rendered progress bar - 100%") {
    val input = 1f

    val result = ProgressBar.renderProgressBar(progress = input)

    result shouldBe "|██████████████████████████████████████████████████| 100%"
  }

}
