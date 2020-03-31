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

import water.*;
import water.exceptions.H2OIllegalArgumentException;
import water.fvec.CStrChunk;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.parser.BufferedString;
import water.util.IcedHashMap;

public class UpdateCategoricalIndicesTask extends MRTask<UpdateCategoricalIndicesTask> {
  private final Key frameKey;
  private final int[] categoricalColumns;

  public UpdateCategoricalIndicesTask(Key frameKey, int[] categoricalColumns) {
    this.frameKey = frameKey;
    this.categoricalColumns = categoricalColumns;
  }

  private static IcedHashMap<BufferedString, Integer> domainToCategoricalMap(String[] domain) {
    IcedHashMap<BufferedString, Integer> categoricalMap = new IcedHashMap<>();
    for (int j = 0; j < domain.length; j++) {
      categoricalMap.put(new BufferedString(domain[j]), j);
    }
    return categoricalMap;
  }

  @Override
  public void map(Chunk[] chunks) {
    Frame frame = DKV.getGet(frameKey);
    int chunkId = chunks[0].cidx();
    if (!LocalNodeDomains.containsDomains(frameKey, chunkId)) {
      throw new H2OIllegalArgumentException(
          String.format(
              "No local domain found for the chunk '%d' on the node '%s'.",
              chunkId, H2O.SELF.getIpPortString()));
    }
    String[][] localDomains = LocalNodeDomains.getDomains(frameKey, chunkId);
    for (int catColIdx = 0; catColIdx < categoricalColumns.length; catColIdx++) {
      int colId = categoricalColumns[catColIdx];
      Chunk chunk = chunks[colId];
      String[] localDomain = localDomains[catColIdx];
      IcedHashMap<BufferedString, Integer> categoricalMap =
          domainToCategoricalMap(frame.vec(colId).domain());
      if (chunk instanceof CStrChunk) continue;
      for (int valIdx = 0; valIdx < chunk._len; ++valIdx) {
        if (chunk.isNA(valIdx)) continue;
        final int oldValue = (int) chunk.at8(valIdx);
        final BufferedString category = new BufferedString(localDomain[oldValue]);
        final int newValue = categoricalMap.get(category);
        chunk.set(valIdx, newValue);
      }
      chunk.close(chunkId, _fs);
    }
  }

  @Override
  public void postGlobal() {
    LocalNodeDomains.remove(frameKey);
  }
}
