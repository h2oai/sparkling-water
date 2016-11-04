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

import hex.*;
import org.apache.spark.mllib.ClusteringUtils;
import water.fvec.Frame;

public class ModelMetricsGaussianMixture extends ModelMetricsUnsupervised {

    public final double _loglikelihood;

    // Don't remove, used by FlowUI
    public double loglikelihood() {
        return _loglikelihood;
    }

    public ModelMetricsGaussianMixture(Model model, Frame frame, double loglikelihood) {
        super(model, frame, 0, Double.NaN);
        this._loglikelihood = loglikelihood;
    }

    public static class MetricBuilderGaussianMixture extends ModelMetricsUnsupervised.MetricBuilderUnsupervised {

        private double _loglikelihood;

        public MetricBuilderGaussianMixture(int ncol) {
            _work = new double[ncol];
        }

        @Override
        public double[] perRow(double[] preds, float[] dataRow, Model m) {
            GaussianMixtureModel gmm = (GaussianMixtureModel) m;
            _loglikelihood = ClusteringUtils.perRowGaussianMixture(dataRow, gmm);
            return preds;
        }

        @Override
        public ModelMetrics makeModelMetrics(Model m, Frame f, Frame adaptedFrame, Frame preds) {
            return m.addMetrics(new ModelMetricsGaussianMixture(m, f, _loglikelihood));
        }
    }

}
