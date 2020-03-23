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

import java.util.ArrayList;

public final class LocalNodeDomains {
    private static NonBlockingHashMap<Key, ArrayList<Categorical[]>> domainsMap = new NonBlockingHashMap<>();
    private static NonBlockingHashMap<String, Categorical[]> domainsMapByChunk = new NonBlockingHashMap<>();
    private static NonBlockingHashMap<Key, ArrayList<String>>  frameKeyToChunkKeys = new NonBlockingHashMap<>();

    public synchronized static void addDomains(Key frameKey, int chunkId, String[][] domains) {
        ArrayList<Categorical[]> nodeDomains = domainsMap.get(frameKey);
        if (nodeDomains == null) {
            nodeDomains = new ArrayList<>();
            domainsMap.put(frameKey, nodeDomains);
        }
        Categorical[] categoricalDomains = domainsToCategoricals(domains);
        nodeDomains.add(categoricalDomains);


        ArrayList<String> chunkKeys = frameKeyToChunkKeys.get(frameKey);
        if (chunkKeys == null) {
            chunkKeys = new ArrayList<>();
            frameKeyToChunkKeys.put(frameKey, chunkKeys);
        }
        String chunkKey = createChunkKey(frameKey, chunkId);
        chunkKeys.add(chunkKey);

        domainsMapByChunk.putIfAbsent(chunkKey, categoricalDomains);
    }

    private static Categorical[] domainsToCategoricals(String[][] domains) {
        Categorical[] result = new Categorical[domains.length];
        for (int i = 0; i < domains.length; i++) {
            String[] domain = domains[i];
            Categorical categorical = new Categorical();
            for (int j = 0; j < domain.length; j++) {
                categorical.addKey(new BufferedString(domain[j]));
            }
            result[i] = categorical;
        }
        return result;
    }

    public synchronized static boolean containsDomains(Key frameKey) {
        return domainsMap.containsKey(frameKey);
    }

    public synchronized static boolean containsDomains(Key frameKey, int chunkId) {
        String chunkKey = createChunkKey(frameKey, chunkId);
        return domainsMapByChunk.containsKey(chunkKey);
    }

    public synchronized static Categorical[][] getDomains(Key frameKey) {
        return domainsMap.get(frameKey).toArray(new Categorical[0][]);
    }

    public synchronized static Categorical[] getDomains(Key frameKey, int chunkId) {
        String chunkKey = createChunkKey(frameKey, chunkId);
        return domainsMapByChunk.get(chunkKey);
    }

    public synchronized static void remove(Key frameKey) {
        if (domainsMap.remove(frameKey) != null) {
            ArrayList<String> chunkKeys = frameKeyToChunkKeys.remove(frameKey);
            for (String chunkKey : chunkKeys) {
                domainsMapByChunk.remove(chunkKey);
            }
            frameKeyToChunkKeys.remove(frameKey);
        }
    }

    private static String createChunkKey(Key frameKey, int chunkId) {
        return frameKey.toString() + "_" + chunkId;
    }
}
