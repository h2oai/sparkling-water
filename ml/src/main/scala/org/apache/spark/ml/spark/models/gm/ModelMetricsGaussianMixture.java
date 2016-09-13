package org.apache.spark.ml.spark.models.gm;

import hex.*;
import water.fvec.Frame;

public class ModelMetricsGaussianMixture extends ModelMetricsUnsupervised {

    public ModelMetricsGaussianMixture(Model model, Frame frame) {
        super(model, frame, 0, Double.NaN);
    }

    public static class MetricBuilderGaussianMixture extends ModelMetricsUnsupervised.MetricBuilderUnsupervised {

        public MetricBuilderGaussianMixture(int ncol, int nclust) {
            _work = new double[ncol];
        }

        @Override
        public double[] perRow(double[] preds, float[] dataRow, Model m) {
            // TODO implement
            return preds;
        }

        @Override
        public void reduce(MetricBuilder mb) {
            ModelMetricsGaussianMixture.MetricBuilderGaussianMixture mm = (ModelMetricsGaussianMixture.MetricBuilderGaussianMixture) mb;
            super.reduce(mm);
            // TODO implement
        }

        @Override
        public ModelMetrics makeModelMetrics(Model m, Frame f, Frame adaptedFrame, Frame preds) {
            assert m instanceof ClusteringModel;
            ClusteringModel clm = (ClusteringModel) m;
            ModelMetricsGaussianMixture mm = new ModelMetricsGaussianMixture(m, f);;

            // TODO implement

            return m.addMetrics(mm);
        }
    }

}
