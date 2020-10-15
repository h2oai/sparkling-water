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
package water.parser

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class CategoricalPreviewParseWriterTestSuite extends FunSuite with Matchers {

  val testCases = Seq(
    Seq.empty[String],
    Seq(null, null),
    Seq(null, "word", null),
    Seq("a", "b", "c", "d", "e", "f", "g", null),
    Seq("a", "b", "b", "a", "a"),
    Seq(null, "a", null, "b", null, "b", null, "a", null, "a", null))

  for (testCase <- testCases) {
    test(s"CategoricalPreviewParseWriter returns expected result on $testCase") {
      val referenceWriter = new PreviewParseWriter(1)
      for (value <- testCase) {
        if (value == null) {
          referenceWriter.addInvalidCol(0)
        } else {
          referenceWriter.addStrCol(0, new BufferedString(value))
        }
        referenceWriter.newLine()
      }
      val expected = referenceWriter.guessTypes()(0)

      val domain = testCase.filter(_ != null).distinct.toArray
      val naCount = testCase.filter(_ == null).length
      val categoricalWriter = new CategoricalPreviewParseWriter(domain, testCase.length, naCount)
      val result = categoricalWriter.guessTypes()(0)

      result shouldEqual expected
    }
  }
}
