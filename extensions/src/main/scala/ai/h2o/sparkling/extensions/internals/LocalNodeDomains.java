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

package ai.h2o.sparkling.extensions.internals;

import java.util.ArrayList;
import water.Key;
import water.nbhm.NonBlockingHashMap;

public final class LocalNodeDomains {
  private static NonBlockingHashMap<Key, ArrayList<String[][]>> domainsMap =
      new NonBlockingHashMap<>();
  private static NonBlockingHashMap<String, String[][]> domainsMapByChunk =
      new NonBlockingHashMap<>();
  private static NonBlockingHashMap<Key, ArrayList<String>> frameKeyToChunkKeys =
      new NonBlockingHashMap<>();

  public static synchronized void addDomains(Key frameKey, int chunkId, String[][] domains) {
    ArrayList<String[][]> nodeDomains = domainsMap.get(frameKey);
    if (nodeDomains == null) {
      nodeDomains = new ArrayList<>();
      domainsMap.put(frameKey, nodeDomains);
    }
    nodeDomains.add(domains);

    ArrayList<String> chunkKeys = frameKeyToChunkKeys.get(frameKey);
    if (chunkKeys == null) {
      chunkKeys = new ArrayList<>();
      frameKeyToChunkKeys.put(frameKey, chunkKeys);
    }
    String chunkKey = createChunkKey(frameKey, chunkId);
    chunkKeys.add(chunkKey);

    domainsMapByChunk.putIfAbsent(chunkKey, domains);
  }

  public static synchronized boolean containsDomains(Key frameKey) {
    return domainsMap.containsKey(frameKey);
  }

  public static synchronized boolean containsDomains(Key frameKey, int chunkId) {
    String chunkKey = createChunkKey(frameKey, chunkId);
    return domainsMapByChunk.containsKey(chunkKey);
  }

  /**
   * The method returns domains for all chunks on the H2O node. The first array level identifies
   * chunks, the second columns, the third column values.
   *
   * @param frameKey A key of a frame containing categorical domains.
   * @return Domains for all chunks on the H2O node.
   */
  public static synchronized String[][][] getDomains(Key frameKey) {
    return domainsMap.get(frameKey).toArray(new String[0][][]);
  }

  public static synchronized String[][] getDomains(Key frameKey, int chunkId) {
    String chunkKey = createChunkKey(frameKey, chunkId);
    return domainsMapByChunk.get(chunkKey);
  }

  public static synchronized void remove(Key frameKey) {
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
