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
package org.apache.spark.ml.spark.models.als;

import hex.ModelBuilder;
import hex.ModelCategory;
import hex.ModelMetrics;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.ml.spark.ProgressListener;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.RDDInfo;
import water.DKV;
import water.fvec.Frame;
import water.fvec.H2OFrame;
import water.fvec.Vec;
import water.util.Log;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static scala.collection.JavaConversions.iterableAsScalaIterable;

public class ALS extends ModelBuilder<ALSModel, ALSParameters, ALSModel.ALSOutput> {

    transient private final H2OContext hc;

    public ALS(boolean startup_once, H2OContext hc) {
        super(new ALSParameters(), startup_once);
        this.hc = hc;
    }

    public ALS(ALSParameters parms, H2OContext hc) {
        super(parms);
        init(false);
        this.hc = hc;
    }

    @Override
    protected Driver trainModelImpl() {
        return new ALS.ALSDriver();
    }

    @Override
    public ModelCategory[] can_build() {
        return new ModelCategory[]{
                ModelCategory.Regression
        };
    }

    @Override
    public boolean isSupervised() {
        return true;
    }

    @Override
    public void init(boolean expensive) {
        super.init(expensive);

        _parms.validate(this);

        if (_train == null) return;

        // TODO move to util class
        for (int i = 0; i < _train.numCols(); i++) {
            Vec vec = _train.vec(i);
            String vecName = _train.name(i);
            if (vec.naCnt() > 0 && (null == _parms._ignored_columns || Arrays.binarySearch(_parms
                    ._ignored_columns, vecName) < 0)) {
                error("_train", "Training frame cannot contain any missing values [" + vecName +
                        "].");
            }
        }

        Set<String> ignoredCols = null != _parms._ignored_columns ?
                new HashSet<String>(Arrays.asList(_parms._ignored_columns)) :
                new HashSet<String>();
        for (int i = 0; i < _train.vecs().length; i++) {
            Vec vec = _train.vec(i);
            if (!ignoredCols.contains(_train.name(i)) && !(vec.isNumeric() || vec.isCategorical()
            )) {
                error("_train", "ALS supports only frames with numeric values (except for result " +
                        "column). But a " + vec.get_type_str() + " was found.");
            }
        }

        if (null != _parms._response_column && null == _train.vec(_parms._response_column)) {
            error("_train", "Training frame has to contain the response column.");
        }
    }


    private final class ALSDriver extends Driver {

        transient private SparkContext sc = hc.sparkContext();
        transient private H2OContext h2oContext = hc;
        transient private SQLContext sqlContext = SQLContext.getOrCreate(sc);

        @Override
        public void computeImpl() {
            init(true);

            // The model to be built
            ALSModel model = null;
            ProgressListener progressBar = null;
            try {
                model = new ALSModel(dest(), _parms, new ALSModel.ALSOutput(ALS.this));
                model.delete_and_lock(_job);

                RDD<Rating> training = getTrainingData(
                        _train,
                        _parms._user_column,
                        _parms._item_column,
                        _parms._response_column
                );
                training.cache();

                org.apache.spark.mllib.recommendation.ALS als =
                        new org.apache.spark.mllib.recommendation.ALS();

                als.setAlpha(_parms._reg_param);
                als.setIterations(_parms._max_iterations);
                als.setSeed(_parms._seed);
                als.setCheckpointInterval(_parms._checkpoint_int);
                als.setImplicitPrefs(_parms._implicit_prefs);
                als.setLambda(_parms._alpha);
                als.setNonnegative(_parms._non_neg);
                als.setProductBlocks(_parms._product_blocks);
                als.setRank(_parms._rank);
                als.setUserBlocks(_parms._user_blocks);


                progressBar = new ProgressListener(sc,
                        _job,
                        RDDInfo.fromRdd(training),
                        // TODO check the stage name for ALS
                        iterableAsScalaIterable(Arrays.asList("treeAggregate")));

                sc.addSparkListener(progressBar);

                MatrixFactorizationModel trainedModel = als.run(training);
                training.unpersist(false);

                model._output.rank_$eq(trainedModel.rank());
                model._output.userFeatures_$eq(trainedModel.userFeatures());
                model._output.productFeatures_$eq(trainedModel.productFeatures());

                Frame train = DKV.<Frame>getGet(_parms._train);
                model.score(train).delete();
                model._output._training_metrics = ModelMetrics.getFromDKV(model, train);

                model.update(_job);

                if (_valid != null) {
                    model.score(_parms.valid()).delete();
                    model._output._validation_metrics =
                            ModelMetrics.getFromDKV(model, _parms.valid());
                    model.update(_job);
                }

                Log.info(model._output._model_summary);
            } finally {
                if(null != model) {
                    model.unlock();
                }
                if(null != progressBar) {
                    sc.listenerBus().listeners().remove(progressBar);
                }
            }
        }

        private RDD<Rating> getTrainingData(Frame parms,
                                            final String _user_column,
                                            final String _product_column,
                                            final String _response_column) {
            Frame trainingSubframe = parms.subframe(
                    new String[]{_user_column, _product_column, _response_column}
            );
            return h2oContext.asDataFrame(new H2OFrame(trainingSubframe), true, sqlContext)
                    .javaRDD()
                    .map(new Function<Row, Rating>() {
                        @Override
                        public Rating call(Row row) throws Exception {
                            // TODO support ratings from Strings, Integers etc. after NA support
                            // gets merged in
                            return new Rating(
                                    row.<Integer>getAs(_user_column),
                                    row.<Integer>getAs(_product_column),
                                    row.<Double>getAs(_response_column)
                            );
                        }
                    })
                    .rdd();
        }
    }

}
