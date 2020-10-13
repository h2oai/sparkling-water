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

import java.lang.reflect.Field;
import water.util.IcedHashMap;

public class CategoricalPreviewParseWriter extends PreviewParseWriter {

  public CategoricalPreviewParseWriter(String[] domain, int totalCount) {
    super(1);
    this._nstrings[0] = totalCount;
    IcedHashMap<String, String>[] domains = getPrivateDomains();
    for (int i = 0; i < domain.length; i++) {
      domains[0].put(domain[i], "");
    }
  }

  private IcedHashMap<String, String>[] getPrivateDomains() {
    IcedHashMap<String, String>[] domains = null;
    try {
      Field domainsField = this.getClass().getDeclaredField("_domains");
      domainsField.setAccessible(true);
      domains = (IcedHashMap<String, String>[]) domainsField.get(this);
      domainsField.setAccessible(false);
    } catch (Exception e) {
    }

    return domains;
  }
}
