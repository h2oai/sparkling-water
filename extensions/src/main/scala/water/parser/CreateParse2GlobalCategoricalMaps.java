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
    private final Key domainsKey;
    private final Key frameKey;
    private final int[] _ecol;
    private final int[] _parseColumns;

    public CreateParse2GlobalCategoricalMaps(Key domainsKey, Key frameKey, int[] ecol, int[] parseColumns) {
        this.domainsKey = domainsKey;
        this.frameKey = frameKey;
        _ecol = ecol; // contains the categoricals column indices only
        _parseColumns = parseColumns;
    }

    @Override
    public void compute2() {
        Frame _fr = DKV.getGet(frameKey);
        // get the node local category->ordinal maps for each column from initial parse pass
        if (!LocalNodeDomains.containsDomains(domainsKey)) {
            tryComplete();
            return;
        }
        final Categorical[] parseCatMaps = LocalNodeDomains.getDomains(domainsKey); // include skipped columns
        int[][] _nodeOrdMaps = new int[_ecol.length][];

        // create old_ordinal->new_ordinal map for each cat column
        for (int eColIdx = 0; eColIdx < _ecol.length; eColIdx++) {
            int colIdx = _parseColumns[_ecol[eColIdx]];
            if (parseCatMaps[colIdx].size() != 0) {
                _nodeOrdMaps[eColIdx] = MemoryManager.malloc4(parseCatMaps[colIdx].maxId() + 1);
                Arrays.fill(_nodeOrdMaps[eColIdx], -1);
                //Bulk String->BufferedString conversion is slightly faster, but consumes memory
                final BufferedString[] unifiedDomain = _fr.vec(_ecol[eColIdx]).isCategorical() ?
                        BufferedString.toBufferedString(_fr.vec(_ecol[eColIdx]).domain()) : new BufferedString[0];
                //final String[] unifiedDomain = _fr.vec(colIdx).domain();
                for (int i = 0; i < unifiedDomain.length; i++) {
                    //final BufferedString cat = new BufferedString(unifiedDomain[i]);
                    if (parseCatMaps[colIdx].containsKey(unifiedDomain[i])) {
                        _nodeOrdMaps[eColIdx][parseCatMaps[colIdx].getTokenId(unifiedDomain[i])] = i;
                    }
                }
            } else {
                Log.debug("Column " + colIdx + " was marked as categorical but categorical map is empty!");
            }
        }
        // Store the local->global ordinal maps in DKV by node parse categorical key and node index
        DKV.put(Key.make(domainsKey.toString() + "parseCatMapNode" + H2O.SELF.index()), new CategoricalUpdateMap(_nodeOrdMaps));
        tryComplete();
    }
}