package org.apache.spark.ml.spark.models.gm;

import hex.ClusteringModelBuilder;
import hex.ModelBuilder;
import hex.ModelCategory;
import hex.kmeans.KMeans;
import water.fvec.Frame;

public class GaussianMixture extends ClusteringModelBuilder<GaussianMixtureModel, GaussianMixtureParameters, GaussianMixtureModel.GaussianMixtureOutput> {

    public GaussianMixture(boolean startup_once) {
        super(new GaussianMixtureParameters(), startup_once);
    }

    public GaussianMixture(GaussianMixtureParameters parms) {
        super(parms);
        init(false);
    }

    @Override public void init(boolean expensive) {
        super.init(expensive);
        if( _parms._max_iterations < 0 || _parms._max_iterations > 1e6) {
            error("_max_iterations", " max_iterations must be between 0 and 1e6");
        }

        if( _parms._convergence_tolerance < 0) {
            error("_convergence_tolerance", " convergence_tolerance must be positive");
        }
    }

    @Override
    protected Driver trainModelImpl() {
        return new GaussianMixtureDriver();
    }

    @Override public ModelCategory[] can_build() { return new ModelCategory[]{ ModelCategory.Clustering }; }

    private class GaussianMixtureDriver extends Driver {

        @Override
        public void computeImpl() {

        }
    }
}
