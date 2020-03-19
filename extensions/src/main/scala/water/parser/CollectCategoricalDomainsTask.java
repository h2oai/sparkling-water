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

import jsr166y.ForkJoinTask;
import water.H2O;
import water.Key;
import water.MRTask;
import water.util.Log;

import java.util.Arrays;

class CollectCategoricalDomainsTask extends MRTask<CollectCategoricalDomainsTask> {
    private final Key key;
    private byte[][] packedDomains;

    private CollectCategoricalDomainsTask(Key key) {
        this.key = key;
    }

    @Override
    public void setupLocal() {
        if (!LocalNodeDomains.containsDomains(key)) return;
        final Categorical[] domains = LocalNodeDomains.getDomains(key);
        packedDomains = new byte[domains.length][];
        int i = 0;
        for (Categorical domain : domains) {
            domain.convertToUTF8(-1);
            BufferedString[] values = domain.getColumnDomain();
            Arrays.sort(values);
            packedDomains[i] = PackedDomains.pack(values);
            i++;
        }
        Log.trace("Done locally collecting domains on each node.");
    }

    @Override
    public void reduce(final CollectCategoricalDomainsTask other) {
        if (packedDomains == null) {
            packedDomains = other.packedDomains;
        } else if (other.packedDomains != null) { // merge two packed domains
            H2O.H2OCountedCompleter[] tasks = new H2O.H2OCountedCompleter[packedDomains.length];
            for (int i = 0; i < this.packedDomains.length; i++) {
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

    public int getDomainLength(int colIdx) {
        return packedDomains == null ? 0 : PackedDomains.sizeOf(packedDomains[colIdx]);
    }

    public String[] getDomain(int colIdx) {
        return packedDomains == null ? null : PackedDomains.unpackToStrings(packedDomains[colIdx]);
    }
}