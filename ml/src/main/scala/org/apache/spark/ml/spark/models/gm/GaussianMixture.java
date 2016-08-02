package org.apache.spark.ml.spark.models.gm;

import hex.ClusteringModelBuilder;
import hex.ModelBuilder;
import hex.ModelCategory;

public class GaussianMixture extends ClusteringModelBuilder<GaussianMixtureModel, GaussianMixtureParameters, GaussianMixtureModel.GaussianMixtureOutput> {

    public GaussianMixture(boolean startup_once) {
        super(new GaussianMixtureParameters(), startup_once);
    }

    public GaussianMixture(GaussianMixtureParameters parms) {
        super(parms);
        init(false);
    }

    @Override
    protected Driver trainModelImpl() {
        return null;
    }

    @Override
    public ModelCategory[] can_build() {
        return new ModelCategory[0];
    }
}
