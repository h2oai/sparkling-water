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
package org.apache.spark.ml.spark.models.gm;

import hex.ClusteringModel;
import org.apache.spark.ml.spark.models.MissingValuesHandling;
import org.apache.spark.util.Utils;

public class GaussianMixtureParameters extends ClusteringModel.ClusteringParameters {
    @Override
    public String algoName() {
        return "GaussianMixture";
    }

    @Override
    public String fullName() {
        return "Gaussian Mixture (*Spark*)";
    }

    @Override
    public String javaName() {
        return GaussianMixtureModel.class.getName();
    }

    @Override
    public long progressUnits() { return _max_iterations; }

    public int _max_iterations = 100;
    public double _convergence_tolerance = 0.01;
    public long _seed = Utils.random().nextLong();
    public boolean _standardize = true;
    public MissingValuesHandling _missing_values_handling = MissingValuesHandling.MeanImputation;

    public void validate(GaussianMixture gm) {
        if (_max_iterations < 0 || _max_iterations > 1e6) {
            gm.error("_max_iterations", " max_iterations must be between 0 and 1e6");
        }
    }

}
