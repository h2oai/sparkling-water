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

import jsr166y.ForkJoinTask;
import water.H2O;
import water.Key;
import water.MRTask;
import water.parser.BufferedString;
import water.parser.PackedDomains;
import water.util.Log;

import java.util.Arrays;

public class CollectCategoricalDomainsTask extends MRTask<CollectCategoricalDomainsTask> {
    private final Key frameKey;
    private byte[][] packedDomains;

    public CollectCategoricalDomainsTask(Key frameKey) {
        this.frameKey = frameKey;
    }

    @Override
    public void setupLocal() {
        if (!LocalNodeDomains.containsDomains(frameKey)) return;
        final String[][][] localDomains = LocalNodeDomains.getDomains(frameKey);
        if (localDomains.length == 0) return;
        packedDomains = chunkDomainsToPackedDomains(localDomains[0]);
        for (int i = 1; i < localDomains.length; i++) {
            byte[][] anotherPackedDomains = chunkDomainsToPackedDomains(localDomains[i]);
            packedDomains = mergePackedDomains(packedDomains, anotherPackedDomains);
        }
        Log.trace("Done locally collecting domains on each node.");
    }

    private byte[][] chunkDomainsToPackedDomains(String[][] domains) {
        byte[][] result = new byte[domains.length][];
        for(int i = 0; i < domains.length; i++) {
            String[] columnDomain = domains[i];
            BufferedString[] values = BufferedString.toBufferedString(columnDomain);
            Arrays.sort(values);
            result[i] = PackedDomains.pack(values);
        }
        return result;
    }

    private byte[][] mergePackedDomains(byte[][] first, byte[][] second) {
        for (int i = 0; i < first.length; i++) {
            first[i] = PackedDomains.merge(first[i], second[i]);
        }
        return first;
    }

    @Override
    public void reduce(final CollectCategoricalDomainsTask other) {
        if (packedDomains == null) {
            packedDomains = other.packedDomains;
        } else if (other.packedDomains != null) { // merge two packed domains
            H2O.H2OCountedCompleter[] tasks = new H2O.H2OCountedCompleter[packedDomains.length];
            for (int i = 0; i < packedDomains.length; i++) {
                final int fi = i;
                tasks[i] = new H2O.H2OCountedCompleter(currThrPriority()) {
                    @Override
                    public void compute2() {
                        packedDomains[fi] = PackedDomains.merge(packedDomains[fi], other.packedDomains[fi]);
                        tryComplete();
                    }
                };
            }
            ForkJoinTask.invokeAll(tasks);
        }
        Log.trace("Done merging domains.");
    }

    public String[] getDomain(int colIdx) {
        return packedDomains == null ? null : PackedDomains.unpackToStrings(packedDomains[colIdx]);
    }

    public String[][] getDomains() {
        if (packedDomains == null) return null;
        String[][] result = new String[packedDomains.length][];
        for (int i = 0; i < packedDomains.length; i++) {
            result[i] = PackedDomains.unpackToStrings(packedDomains[i]);
        }
        return result;
    }
}
