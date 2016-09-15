package org.apache.spark.ml.spark.models.gm;

import hex.*;
import org.apache.spark.mllib.ClusteringUtils;
import water.fvec.Frame;

public class ModelMetricsGaussianMixture extends ModelMetricsUnsupervised {

    public final double _loglikelihood;

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
