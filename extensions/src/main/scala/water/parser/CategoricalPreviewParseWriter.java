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

package water.parser;

public class CategoricalPreviewParseWriter {

  public static byte guessType(String[] domain, int nLines, int nEmpty) {
    final int nStrings = nLines - nEmpty;
    final int nNums = 0;
    final int nDates = 0;
    final int nUUID = 0;
    final int nZeros = 0;

    PreviewParseWriter.IDomain domainWrapper =
        new PreviewParseWriter.IDomain() {
          public int size() {
            return domain.length;
          }

          public boolean contains(String value) {
            for (String domainValue : domain) {
              if (value.equals(domainValue)) return true;
            }
            return false;
          }
        };

    return PreviewParseWriter.guessType(
        nLines, nNums, nStrings, nDates, nUUID, nZeros, nEmpty, domainWrapper);
  }
}
