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

package ai.h2o.sparkling.ml.internals;

public enum H2OMetric {
  AUTO(true),
  MeanResidualDeviance(false),
  R2(true),
  ResidualDeviance(false),
  ResidualDegreesOfFreedom(false),
  NullDeviance(false),
  NullDegreesOfFreedom(false),
  AIC(true),
  AUC(true),
  Gini(true),
  F1(true),
  F2(true),
  F0point5(true),
  Precision(true),
  Recall(true),
  MCC(true),
  Logloss(false),
  Error(false),
  MaxPerClassError(false),
  Accuracy(true),
  MSE(false),
  RMSE(false),
  Withinss(false),
  Betweenss(true),
  TotWithinss(false),
  Totss(false),
  GLRMMetric(false),
  PCAMetric(true);

  public boolean higherTheBetter() {
    return higherTheBetter;
  }

  private boolean higherTheBetter;

  H2OMetric(boolean higherTheBetter) {
    this.higherTheBetter = higherTheBetter;
  }
}
