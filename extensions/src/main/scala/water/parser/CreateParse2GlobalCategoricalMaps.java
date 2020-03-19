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

import water.*;
import water.fvec.Frame;
import water.util.Log;

import java.util.Arrays;

public class CreateParse2GlobalCategoricalMaps extends DTask<CreateParse2GlobalCategoricalMaps> {
    private final Key frameKey;
    private final int chunkId;
    private final int[] categoricalColumns;

    public CreateParse2GlobalCategoricalMaps(Key frameKey, int chunkId,  int[] categoricalColumns) {
        this.frameKey = frameKey;
        this.chunkId = chunkId;
        this.categoricalColumns = categoricalColumns;
    }

    @Override
    public void compute2() {
        Frame _fr = DKV.getGet(frameKey);
        // get the node local category->ordinal maps for each column from initial parse pass
        if (!LocalNodeDomains.containsDomains(frameKey, chunkId)) {
            tryComplete();
            return;
        }
        final Categorical[] parseCatMaps = LocalNodeDomains.getDomains(frameKey, chunkId);
        int[][] chunkOrdMaps = new int[categoricalColumns.length][];

        // create old_ordinal->new_ordinal map for each cat column
        for (int catColIdx = 0; catColIdx < categoricalColumns.length; catColIdx++) {
            int colIdx = categoricalColumns[catColIdx];
            if (parseCatMaps[colIdx].size() != 0) {
                chunkOrdMaps[catColIdx] = MemoryManager.malloc4(parseCatMaps[colIdx].maxId() + 1);
                Arrays.fill(chunkOrdMaps[catColIdx], -1);
                final BufferedString[] unifiedDomain = _fr.vec(categoricalColumns[catColIdx]).isCategorical() ?
                        BufferedString.toBufferedString(_fr.vec(categoricalColumns[catColIdx]).domain()) : new BufferedString[0];

                for (int i = 0; i < unifiedDomain.length; i++) {
                    if (parseCatMaps[colIdx].containsKey(unifiedDomain[i])) {
                        chunkOrdMaps[catColIdx][parseCatMaps[colIdx].getTokenId(unifiedDomain[i])] = i;
                    }
                }
            } else {
                Log.debug("Column " + colIdx + " was marked as categorical but categorical map is empty!");
            }
        }
        // Store the local->global ordinal maps in DKV by node parse categorical key and node index
        DKV.put(Key.make(frameKey.toString() + "parseCatMapNode" + chunkId), new CategoricalUpdateMap(chunkOrdMaps));
        tryComplete();
    }
}
