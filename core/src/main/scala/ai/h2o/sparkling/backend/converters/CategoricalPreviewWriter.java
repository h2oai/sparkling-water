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

package ai.h2o.sparkling.backend.converters;

import java.util.HashMap;
import water.fvec.Vec;
import water.parser.*;

import java.io.Serializable;

/**
 * The logic is a copy-pasted piece from H2O-3's PreviewParserWriter. PreviewParseWriter can't be used directly
 * since utilize Iced serialization, which requires running H2O.
 */
class CategoricalPreviewWriter implements Serializable {
    private int nlines;
    private int ncols;
    private HashMap<String,String>[] domains;
    private int[] nnums;
    private int[] nstrings;
    private int[] ndates;
    private int[] nUUID;
    private int[] nzeros;
    private int[] nempty;

    public CategoricalPreviewWriter() { super(); }

    public CategoricalPreviewWriter(int n) {
        ncols = n;
        nzeros = new int[n];
        nstrings = new int[n];
        nUUID = new int[n];
        ndates = new int[n];
        nnums = new int[n];
        nempty = new int[n];
        domains = new HashMap[n];
        for(int i = 0; i < n; ++i) {
            domains[i] = new HashMap<>();
        }
    }

    static final int MAX_PREVIEW_RECORDS = 100;

    public void addInvalidCol(int colIdx) {
        if(colIdx < ncols) {
            ++nempty[colIdx];
        }
    }

    public void addStrCol(int colIdx, BufferedString str) {
        if(colIdx < ncols) {
            // Check for time
            if (ParseTime.isTime(str)) {
                ++ndates[colIdx];
                return;
            }

            //Check for UUID
            if(ParseUUID.isUUID(str)) {
                ++nUUID[colIdx];
                return;
            }

            //Add string to domains list for later determining string, NA, or categorical
            ++nstrings[colIdx];
            domains[colIdx].put(str.toString(),"");
        }
    }

    public byte[] guessTypes() {
        byte[] types = new byte[ncols];
        for (int i = 0; i < ncols; ++i) {
            int nonemptyLines = nlines - nempty[i] - 1; //During guess, some columns may be shorted one line based on 4M boundary

            //Very redundant tests, but clearer and not speed critical

            // is it clearly numeric?
            if ((nnums[i] + nzeros[i]) >= ndates[i]
                    && (nnums[i] + nzeros[i]) >= nUUID[i]
                    && nnums[i] >= nstrings[i]) { // 0s can be an NA among categoricals, ignore
                types[i] = Vec.T_NUM;
                continue;
            }

            // All same string or empty?
            if (domains[i].size() == 1 && ndates[i]==0 ) {
                // Obvious NA, or few instances of the single string, declare numeric
                // else categorical
                types[i] = (domains[i].containsKey("NA") ||
                        domains[i].containsKey("na") ||
                        domains[i].containsKey("Na") ||
                        domains[i].containsKey("N/A") ||
                        nstrings[i] < nnums[i] + nzeros[i]) ? Vec.T_NUM : Vec.T_CAT;
                continue;
            }

            // with NA, but likely numeric
            if (domains[i].size() <= 1 && (nnums[i] + nzeros[i]) > ndates[i] + nUUID[i]) {
                types[i] = Vec.T_NUM;
                continue;
            }

            // Datetime
            if (ndates[i] > nUUID[i]
                    && ndates[i] > (nnums[i] + nzeros[i])
                    && (ndates[i] > nstrings[i] || domains[i].size() <= 1)) {
                types[i] = Vec.T_TIME;
                continue;
            }

            // UUID
            if (nUUID[i] > ndates[i]
                    && nUUID[i] > (nnums[i] + nzeros[i])
                    && (nUUID[i] > nstrings[i] || domains[i].size() <= 1)) {
                types[i] = Vec.T_UUID;
                continue;
            }

            // Strings, almost no dups
            if (nstrings[i] > ndates[i]
                    && nstrings[i] > nUUID[i]
                    && nstrings[i] > (nnums[i] + nzeros[i])
                    && domains[i].size() >= 0.95 * nstrings[i]) {
                types[i] = Vec.T_STR;
                continue;
            }

            // categorical or string?
            // categorical with 0s for NAs
            if(nzeros[i] > 0
                    && ((nzeros[i] + nstrings[i]) >= nonemptyLines) //just strings and zeros for NA (thus no empty lines)
                    && (domains[i].size() <= 0.95 * nstrings[i]) ) { // not all unique strings
                types[i] = Vec.T_CAT;
                continue;
            }
            // categorical mixed with numbers
            if(nstrings[i] >= (nnums[i] + nzeros[i]) // mostly strings
                    && (domains[i].size() <= 0.95 * nstrings[i]) ) { // but not all unique
                types[i] = Vec.T_CAT;
                continue;
            }

            // All guesses failed
            types[i] = Vec.T_NUM;
        }
        return types;
    }

    public static CategoricalPreviewWriter unifyColumnPreviews(CategoricalPreviewWriter prevA, CategoricalPreviewWriter prevB) {
        if (prevA == null) return prevB;
        else if (prevB == null) return prevA;
        else {
            //sanity checks
            if (prevA.ncols != prevB.ncols)
                throw new ParseDataset.H2OParseException("Files conflict in number of columns. "
                        + prevA.ncols + " vs. " + prevB.ncols + ".");
            prevA.nlines += prevB.nlines;
            for (int i = 0; i < prevA.ncols; i++) {
                prevA.nnums[i] += prevB.nnums[i];
                prevA.nstrings[i] += prevB.nstrings[i];
                prevA.ndates[i] += prevB.ndates[i];
                prevA.nUUID[i] += prevB.nUUID[i];
                prevA.nzeros[i] += prevB.nzeros[i];
                prevA.nempty[i] += prevB.nempty[i];
                if (prevA.domains[i] != null) {
                    if (prevB.domains[i] != null)
                        for(String s:prevB.domains[i].keySet())
                            prevA.domains[i].put(s,"");
                } else if (prevB.domains[i] != null)
                    prevA.domains = prevB.domains;
            }
        }
        return prevA;
    }
}
