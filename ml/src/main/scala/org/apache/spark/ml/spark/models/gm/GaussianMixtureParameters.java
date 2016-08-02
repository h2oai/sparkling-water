package org.apache.spark.ml.spark.models.gm;

import hex.ClusteringModel;

public class GaussianMixtureParameters extends ClusteringModel.ClusteringParameters {
    @Override
    public String algoName() {
        return "GaussianMixture";
    }

    @Override
    public String fullName() {
        return "Gaussian Mixture";
    }

    @Override
    public String javaName() {
        return GaussianMixtureModel.class.getName();
    }

    @Override
    public long progressUnits() { return _max_iterations; }

    public int _max_iterations = 1000;

}
