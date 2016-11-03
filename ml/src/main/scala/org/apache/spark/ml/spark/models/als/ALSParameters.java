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
package org.apache.spark.ml.spark.models.als;

import hex.Model;

public class ALSParameters extends Model.Parameters {
    public int _max_iterations = 1000;
    public double _reg_param = 0.01;
    public String _user_column;
    public String _item_column;
    public int _checkpoint_int;
    public boolean _implicit_prefs;
    public double _alpha;
    public boolean _non_neg;
    public int _product_blocks;
    public int _rank;
    public int _user_blocks;

    @Override
    public String algoName() {
        return "als";
    }

    @Override
    public String fullName() {
        return "Alternating Least Squares (*Spark*)";
    }

    @Override
    public String javaName() {
        return ALSModel.class.getName();
    }

    @Override
    public long progressUnits() {
        return _max_iterations;
    }

    public void validate(ALS als) {
        if (_max_iterations < 0 || _max_iterations > 1e6) {
            als.error("_max_iterations", "max_iterations must be between 0 and 1e6");
        }

        nonNegativeMinusOne(als, _user_blocks, "user_blocks");
        nonNegativeMinusOne(als, _product_blocks, "product_blocks");

        if (_reg_param < 0) {
            als.error("_reg_param", "reg_param must be positive");
        }

        if (_rank < 0) {
            als.error("_rank", "rank must be positive");
        }
    }

    private void nonNegativeMinusOne(ALS als, int val, String fieldName) {
        if (val == -1 || val > 0) {
            als.error("_" + fieldName, fieldName + " must be -1 or positive");
        }
    }
}
