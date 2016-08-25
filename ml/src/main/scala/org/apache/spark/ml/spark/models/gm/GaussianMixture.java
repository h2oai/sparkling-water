package org.apache.spark.ml.spark.models.gm;

import hex.ClusteringModelBuilder;
import hex.ModelCategory;
import hex.ModelMetrics;
import org.apache.spark.SparkContext;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import water.DKV;
import water.fvec.Frame;
import water.fvec.H2OFrame;
import water.fvec.Vec;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.spark.api.java.function.Function;
import water.util.Log;

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
        _parms.validate(this);

        if (_train == null) return;

        // TODO the below should be refactored into a separate class/util when SVM and this branch get merged
        for (int i = 0; i < _train.numCols(); i++) {
            Vec vec = _train.vec(i);
            String vecName = _train.name(i);
            if (vec.naCnt() > 0 && (null == _parms._ignored_columns || Arrays.binarySearch(_parms._ignored_columns, vecName) < 0)) {
                error("_train", "Training frame cannot contain any missing values [" + vecName + "].");
            }
        }

        Set<String> ignoredCols = null != _parms._ignored_columns ?
                new HashSet<String>(Arrays.asList(_parms._ignored_columns)) :
                new HashSet<String>();
        for (int i = 0; i < _train.vecs().length; i++) {
            Vec vec = _train.vec(i);
            if (!ignoredCols.contains(_train.name(i)) && !(vec.isNumeric() || vec.isCategorical())) {
                error("_train", "SVM supports only frames with numeric values (except for result column). But a " + vec.get_type_str() + " was found.");
            }
        }
    }

    @Override
    protected Driver trainModelImpl() {
        return new GaussianMixtureDriver();
    }

    @Override public ModelCategory[] can_build() { return new ModelCategory[]{ ModelCategory.Clustering }; }

    private class GaussianMixtureDriver extends Driver {

        transient private H2OContext h2oContext = H2OContext.get().get();
        transient private SparkContext sc = h2oContext.sparkContext();
        transient private SQLContext sqlContext = SQLContext.getOrCreate(sc);

        @Override
        public void computeImpl() {
            init(true);

            GaussianMixtureModel model = new GaussianMixtureModel(
                    dest(),
                    _parms,
                    new GaussianMixtureModel.GaussianMixtureOutput(GaussianMixture.this)
            );

            model.delete_and_lock(_job);

            org.apache.spark.mllib.clustering.GaussianMixture sparkGM =
                    new org.apache.spark.mllib.clustering.GaussianMixture();

            sparkGM.setConvergenceTol(_parms._convergence_tolerance);
            sparkGM.setK(_parms._k);
            sparkGM.setMaxIterations(_parms._max_iterations);
            sparkGM.setSeed(_parms._seed);

            RDD<Vector> trainingData = getTrainingData(_train);
            trainingData.cache();

            org.apache.spark.mllib.clustering.GaussianMixtureModel sparkGMModel = sparkGM.run(trainingData);
            trainingData.unpersist(false);

            model._output._iterations_$eq(_parms._max_iterations);
            model._output._weights_$eq(sparkGMModel.weights());
            model._output._mu_$eq(new double[sparkGMModel.gaussians().length][]);
            model._output._sigma_$eq(new double[sparkGMModel.gaussians().length][]);
            model._output._sigma_cols_$eq(new int[sparkGMModel.gaussians().length]);
            for (int i = 0; i < sparkGMModel.gaussians().length; i++) {
                model._output._mu()[i] = sparkGMModel.gaussians()[i].mu().toArray();
                model._output._sigma()[i] = sparkGMModel.gaussians()[i].sigma().toArray();
                model._output._sigma_cols()[i] = sparkGMModel.gaussians()[i].sigma().numCols();
            }

            model.init();

//            Frame train = DKV.<Frame>getGet(_parms._train);
//            model.score(train).delete();
//            model._output._training_metrics = ModelMetrics.getFromDKV(model, train);

            model.update(_job);

            _job.update(model._parms._max_iterations);

            if (_valid != null) {
//                model.score(_parms.valid()).delete();
//                model._output._validation_metrics = ModelMetrics.getFromDKV(model, _parms.valid());
//                model.update(_job);
            }

            Log.info(model._output._model_summary);

        }

        private RDD<Vector> getTrainingData(Frame training) {
            return h2oContext.asSchemaRDD(new H2OFrame(training), sqlContext)
                    .javaRDD()
                    .map(new RowToLabeledPoint(training.domains()))
                    .rdd();
        }
    }
}

class RowToLabeledPoint implements Function<Row, Vector> {

    private String[][] domains;

    RowToLabeledPoint(String[][] domains) {
        this.domains = domains;
    }

    @Override
    public Vector call(Row row) throws Exception {
        StructField[] fields = row.schema().fields();
        double[] features = new double[row.length()];
        for (int i = 0; i < row.length(); i++) {
            features[i] = toDouble(row.get(i), fields[i], domains[i]);
        }
        return Vectors.dense(features);
    }

    // TODO off to util class when merged with SVM
    private double toDouble(Object value, StructField fieldStruct, String[] domain) {
        if (fieldStruct.dataType().sameType(DataTypes.ByteType)) {
            return ((Byte) value).doubleValue();
        }

        if (fieldStruct.dataType().sameType(DataTypes.ShortType)) {
            return ((Short) value).doubleValue();
        }

        if (fieldStruct.dataType().sameType(DataTypes.IntegerType)) {
            return ((Integer) value).doubleValue();
        }

        if (fieldStruct.dataType().sameType(DataTypes.DoubleType)) {
            return (Double) value;
        }

        if (fieldStruct.dataType().sameType(DataTypes.StringType)) {
            return Arrays.binarySearch(domain, value);
        }

        throw new IllegalArgumentException("Target column has to be an enum or a number. " + fieldStruct.toString());
    }
}