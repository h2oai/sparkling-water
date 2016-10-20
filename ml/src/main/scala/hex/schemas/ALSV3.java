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

import org.apache.spark.ml.spark.models.als.ALS;
import org.apache.spark.ml.spark.models.als.ALSParameters;
import water.api.API;
import water.api.schemas3.FrameV3;
import water.api.schemas3.ModelParametersSchemaV3;

public class ALSV3 extends ModelBuilderSchema<ALS, ALSV3, ALSV3.ALSParametersV3> {

    public static final class ALSParametersV3 extends
            ModelParametersSchemaV3<ALSParameters, ALSV3.ALSParametersV3> {
        public static String[] fields = new String[]{
                "model_id",
                "training_frame",
                "user_column",
                "item_column",
                "response_column",
                "validation_frame",
                "nfolds",

                "reg_param",
                "max_iterations",
                "user_column",
                "item_column",
                "rank",
                "checkpoint_int",
                "implicit_prefs",
                "alpha",
                "non_neg",
                "product_blocks",
                "user_blocks",

                "ignored_columns"
        };

        @API(help="Maximum training iterations", gridable = true)
        public int max_iterations = 10;

        @API(level = API.Level.critical, direction = API.Direction.INOUT, gridable = true,
                is_member_of_frames = {"training_frame", "validation_frame"},
                is_mutually_exclusive_with = {"ignored_columns"},
                help = "User variable column.")
        public FrameV3.ColSpecifierV3 user_column;

        @API(level = API.Level.critical, direction = API.Direction.INOUT, gridable = true,
                is_member_of_frames = {"training_frame", "validation_frame"},
                is_mutually_exclusive_with = {"ignored_columns"},
                help = "Item variable column.")
        public FrameV3.ColSpecifierV3 item_column;

        @API(help = "Set the rank of the feature matrices computed.",
                direction = API.Direction.INPUT, gridable = true, level = API.Level.critical)
        public int rank = 10;

        @API(help = "Set the regularization parameter, lambda.", direction = API.Direction.INPUT,
                gridable = true, level = API.Level.expert)
        public double reg_param = 0.01;

        @API(help = "Set period (in iterations) between checkpoints",
                direction = API.Direction.INPUT, level = API.Level.expert)
        public int checkpoint_int = 10;

        @API(help = "Sets whether to use implicit preference.", direction = API.Direction.INPUT,
                gridable = true, level = API.Level.expert)
        public boolean implicit_prefs = false;

        @API(help = "Sets the constant used in computing confidence in implicit ALS.",
                direction = API.Direction.INPUT, gridable = true, level = API.Level.expert)
        public double alpha = 1.0;

        @API(help = "Set whether the least-squares problems solved at each iteration should have " +
                "nonnegativity constraints.", direction = API.Direction.INPUT,
                gridable = true, level = API.Level.expert)
        public boolean non_neg = false;

        @API(help = "Set the number of product blocks to parallelize the computation.",
                direction = API.Direction.INPUT, gridable = true, level = API.Level.expert)
        public int product_blocks = -1;

        @API(help = "Set the number of user blocks to parallelize the computation.",
                direction = API.Direction.INPUT, gridable = true, level = API.Level.expert)
        public int user_blocks = -1;
    }
}
