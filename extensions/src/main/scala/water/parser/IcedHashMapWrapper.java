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

import water.util.IcedHashMap;

public class IcedHashMapWrapper extends IcedHashMap<String, String> {
    private String[] _values = null;

    public IcedHashMapWrapper(String[] values) {
        this._values = values;
    }

    @Override public int size() {
        return _values.length;
    }

    @Override
    public boolean containsKey(Object key) {
        for (String value: this._values) {
            if (value.equals(key)) return true;
        }
        return false;
    }
}
