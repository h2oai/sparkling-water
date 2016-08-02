package hex.schemas;

import org.apache.spark.ml.spark.models.gm.GaussianMixture;
import org.apache.spark.ml.spark.models.gm.GaussianMixtureParameters;
import water.api.schemas3.ClusteringModelParametersSchemaV3;

public class GaussianMixtureV3 extends
        ClusteringModelBuilderSchema<GaussianMixture, GaussianMixtureV3, GaussianMixtureV3.GaussianMixtureParametersV3> {

    public static final class GaussianMixtureParametersV3 extends
            ClusteringModelParametersSchemaV3<GaussianMixtureParameters, GaussianMixtureParametersV3> {
                public static String[] fields = new String[]{
                        // TODO add field names
                };

        // TODO add input fields
    }
}