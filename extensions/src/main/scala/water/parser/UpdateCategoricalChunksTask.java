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

import water.DKV;
import water.H2O;
import water.Key;
import water.MRTask;
import water.exceptions.H2OIllegalValueException;
import water.fvec.CStrChunk;
import water.fvec.Chunk;
import water.util.Log;
import water.util.PrettyPrint;

class UpdateCategoricalChunksTask extends MRTask<UpdateCategoricalChunksTask> {
    private final Key _parseCatMapsKey;
    private final int[] _chunk2ParseNodeMap;

    private UpdateCategoricalChunksTask(Key parseCatMapsKey, int[] chunk2ParseNodeMap) {
        _parseCatMapsKey = parseCatMapsKey;
        _chunk2ParseNodeMap = chunk2ParseNodeMap;
    }

    @Override
    public void map(Chunk[] chks) {
        CategoricalUpdateMap temp = DKV.getGet(Key.make(_parseCatMapsKey.toString() + "parseCatMapNode" + _chunk2ParseNodeMap[chks[0].cidx()]));
        if (temp == null || temp.map == null)
            throw new H2OIllegalValueException("Missing categorical update map", this);
        int[][] _parse2GlobalCatMaps = temp.map;

        //update the chunk with the new map
        final int cidx = chks[0].cidx();
        for (int i = 0; i < chks.length; ++i) {
            Chunk chk = chks[i];
            if (!(chk instanceof CStrChunk)) {
                for (int j = 0; j < chk._len; ++j) {
                    if (chk.isNA(j)) continue;
                    final int old = (int) chk.at8(j);
                    if (old < 0 || (_parse2GlobalCatMaps[i] != null && old >= _parse2GlobalCatMaps[i].length))
                        chk.reportBrokenCategorical(i, j, old, _parse2GlobalCatMaps[i], _fr.vec(i).domain().length);
                    if (_parse2GlobalCatMaps[i] != null && _parse2GlobalCatMaps[i][old] < 0)
                        throw new ParseDataset.H2OParseException("Error in unifying categorical values. This is typically "
                                + "caused by unrecognized characters in the data.\n The problem categorical value "
                                + "occurred in the " + PrettyPrint.withOrdinalIndicator(i + 1) + " categorical col, "
                                + PrettyPrint.withOrdinalIndicator(chk.start() + j) + " row.");
                    if (_parse2GlobalCatMaps[i] != null)
                        chk.set(j, _parse2GlobalCatMaps[i][old]);
                }
                Log.trace("Updated domains for " + PrettyPrint.withOrdinalIndicator(i + 1) + " categorical column.");
            }
            chk.close(cidx, _fs);
        }
    }

    @Override
    public void postGlobal() {
        for (int i = 0; i < H2O.CLOUD.size(); i++)
            DKV.remove(Key.make(_parseCatMapsKey.toString() + "parseCatMapNode" + i));
    }
}