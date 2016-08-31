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
package org.apache.spark.ml.spark.models.svm;

import org.apache.spark.mllib.optimization.HingeGradient;
import org.apache.spark.mllib.optimization.LeastSquaresGradient;
import org.apache.spark.mllib.optimization.LogisticGradient;

public enum Gradient {
    Hinge(new HingeGradient()),
    LeastSquares(new LeastSquaresGradient()),
    Logistic(new LogisticGradient());

    private org.apache.spark.mllib.optimization.Gradient sparkGradient;

    Gradient(org.apache.spark.mllib.optimization.Gradient sparkGradient) {
        this.sparkGradient = sparkGradient;
    }

    public org.apache.spark.mllib.optimization.Gradient get() {
        return sparkGradient;
    }
}
