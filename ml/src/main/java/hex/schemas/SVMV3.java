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

package hex.schemas;

import org.apache.spark.ml.spark.models.MissingValuesHandling;
import org.apache.spark.ml.spark.models.svm.*;
import water.DKV;
import water.Key;
import water.Value;
import water.api.API;
import water.api.schemas3.KeyV3;
import water.api.schemas3.ModelParametersSchemaV3;
import water.fvec.Frame;

// Seems like this has to be in Java since H2O's frameworks uses reflection's getFields...
// I probably could mix Java and Scala here, leave SVMParametersV3 with fields as Java
// and then make the same Scala class SVMParametersV3 which extends it but not sure if it's worth it...
public class SVMV3 extends ModelBuilderSchema<SVM, SVMV3, SVMV3.SVMParametersV3> {

    public static final class SVMParametersV3 extends
            ModelParametersSchemaV3<SVMParameters, SVMParametersV3> {
        public static String[] fields = new String[]{
                "model_id",
                "training_frame",
                "response_column",
                "initial_weights_frame",
                "validation_frame",
                "nfolds",
                "add_intercept",

                "step_size",
                "reg_param",
                "convergence_tol",
                "mini_batch_fraction",
                "threshold",
                "updater",
                "gradient",

                "ignored_columns",
                "ignore_const_cols",
                "missing_values_handling"
        };

        @API(help="Initial model weights.", direction=API.Direction.INOUT, gridable = true)
        public KeyV3.FrameKeyV3 initial_weights_frame;

        @API(help="Add intercept.", direction=API.Direction.INOUT, gridable = true, level = API.Level.expert)
        public boolean add_intercept = false;

        @API(help="Set step size", direction=API.Direction.INPUT, gridable = true, level = API.Level.expert)
        public double step_size = 1.0;

        @API(help="Set regularization parameter", direction=API.Direction.INPUT, gridable = true, level = API.Level.expert)
        public double reg_param = 0.01;

        @API(help="Set convergence tolerance", direction=API.Direction.INPUT, gridable = true, level = API.Level.expert)
        public double convergence_tol = 0.001;

        @API(help="Set mini batch fraction", direction=API.Direction.INPUT, gridable = true, level = API.Level.expert)
        public double mini_batch_fraction = 1.0;

        // TODO what exactly does INOUT do?? Should this be only INPUT?
        @API(help="Set threshold that separates positive predictions from negative ones. NaN for raw prediction.", direction=API.Direction.INOUT, gridable = true, level = API.Level.expert)
        public double threshold = 0.0;

        @API(help="Set the updater for SGD.", direction=API.Direction.INPUT, values = {"L2", "L1", "Simple"}, required = true, gridable = true, level = API.Level.expert)
        public Updater updater = Updater.L2;

        @API(help="Set the gradient computation type for SGD.", direction=API.Direction.INPUT, values = {"Hinge", "LeastSquares", "Logistic"}, required = true, gridable = true, level = API.Level.expert)
        public Gradient gradient = Gradient.Hinge;

        @API(level = API.Level.expert, direction = API.Direction.INOUT, gridable = true,
                values = {"NotAllowed", "Skip", "MeanImputation"},
                help = "Handling of missing values. Either NotAllowed, Skip or MeanImputation.")
        public MissingValuesHandling missing_values_handling;

        @Override
        public SVMParametersV3 fillFromImpl(SVMParameters impl) {
            super.fillFromImpl(impl);

            if (null != impl._initial_weights) {
                Value v = DKV.get(impl._initial_weights);
                if (null != v) {
                    initial_weights_frame = new KeyV3.FrameKeyV3(((Frame) v.get())._key);
                }
            }

            return this;
        }

        @Override
        public SVMParameters fillImpl(SVMParameters impl) {
            super.fillImpl(impl);
            impl._initial_weights =
                    null == this.initial_weights_frame ? null : Key.<Frame>make(this.initial_weights_frame.name);
            return impl;
        }

    }

}
