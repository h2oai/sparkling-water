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

import water.H2O;
import water.Key;
import water.MRTask;
import water.exceptions.H2OIllegalArgumentException;
import water.fvec.Chunk;
import water.fvec.NewChunk;

public class ConvertCategoricalToStringColumnsTask
    extends MRTask<ConvertCategoricalToStringColumnsTask> {
  private final Key frameKey;
  private final int[] domainIndices;

  public ConvertCategoricalToStringColumnsTask(Key frameKey, int[] domainIndices) {
    this.frameKey = frameKey;
    this.domainIndices = domainIndices;
  }

  @Override
  public void map(Chunk[] chunks, NewChunk[] newChunks) {
    int chunkId = chunks[0].cidx();
    if (!LocalNodeDomains.containsDomains(frameKey, chunkId)) {
      throw new H2OIllegalArgumentException(
          String.format(
              "No local domain found for the chunk '%d' on the node '%s'.",
              chunkId, H2O.SELF.getIpPortString()));
    }
    String[][] localDomains = LocalNodeDomains.getDomains(frameKey, chunkId);
    for (int colIdx = 0; colIdx < chunks.length; colIdx++) {
      int domainIdx = domainIndices[colIdx];
      Chunk chunk = chunks[colIdx];
      NewChunk newChunk = newChunks[colIdx];
      String[] localDomain = localDomains[domainIdx];
      for (int valIdx = 0; valIdx < chunk._len; ++valIdx) {
        if (chunk.isNA(valIdx)) {
          newChunk.addNA();
        } else {
          final int oldValue = (int) chunk.at8(valIdx);
          final String category = localDomain[oldValue];
          newChunk.addStr(category);
        }
      }
    }
  }

  @Override
  public void closeLocal() {
    super.closeLocal();
    LocalNodeDomains.remove(frameKey, domainIndices);
  }
}
