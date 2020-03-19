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

import water.Key;
import water.nbhm.NonBlockingHashMap;

public final class LocalNodeDomains {
    private static NonBlockingHashMap<Key, Categorical[]> domainsMap = new NonBlockingHashMap<>();

    public static void addDomains(String frameName, int chunkId, String[][] domains) {
        Key key = createDomainKey(frameName, chunkId);
        Categorical[] result = new Categorical[domains.length];
        for (int i = 0; i < domains.length; i++) {
            String[] domain = domains[i];
            Categorical categorical = new Categorical();
            for (int j = 0; j < domain.length; j++) {
                categorical.addKey(new BufferedString(domain[j]));
            }
            result[i] = categorical;
        }
        domainsMap.put(key, result);
    }

    public static boolean containsDomains(Key key) {
        return domainsMap.containsKey(key);
    }

    public static Categorical[] getDomains(Key key) {
        return domainsMap.get(key);
    }

    public static Key createDomainKey(String frameName, int chunkId) {
        return Key.make(frameName + "_" + chunkId);
    }
}
