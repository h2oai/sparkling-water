package hex.schemas;

import org.apache.spark.ml.spark.models.MissingValuesHandling;
import org.apache.spark.ml.spark.models.gm.GaussianMixture;
import org.apache.spark.ml.spark.models.gm.GaussianMixtureParameters;
import org.apache.spark.util.Utils;
import water.api.API;
import water.api.schemas3.ClusteringModelParametersSchemaV3;

public class GaussianMixtureV3 extends
        ClusteringModelBuilderSchema<GaussianMixture, GaussianMixtureV3, GaussianMixtureV3.GaussianMixtureParametersV3> {

    public static final class GaussianMixtureParametersV3 extends
            ClusteringModelParametersSchemaV3<GaussianMixtureParameters, GaussianMixtureParametersV3> {
        static public String[] fields = new String[] {
                "model_id",
                "training_frame",
                "validation_frame",
                "nfolds",
                "keep_cross_validation_predictions",
                "keep_cross_validation_fold_assignment",
                "fold_assignment",
                "fold_column",
                "ignored_columns",
                "k",
                "convergence_tolerance",
                "seed",
                "missing_values_handling"
        };

        @API(help = "Convergence tolerance", level = API.Level.secondary, gridable = true)
        public double convergence_tolerance = 0.01;

        @API(help = "RNG Seed", level = API.Level.expert, gridable = true)
        public long seed = Utils.random().nextLong();

        @API(level = API.Level.expert, direction = API.Direction.INOUT, gridable = true,
                values = {"NotAllowed", "Skip", "MeanImputation"},
                help = "Handling of missing values. Either NotAllowed, Skip or MeanImputation.")
        public MissingValuesHandling missing_values_handling;
    }


}