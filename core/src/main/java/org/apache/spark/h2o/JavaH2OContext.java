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

package org.apache.spark.h2o;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.h2o.converters.SupportedRDD$;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Option;
import water.Key;
import water.fvec.Frame;
import water.fvec.H2OFrame;

/**
 * A Java-friendly version of [[org.apache.spark.h2o.H2OContext]]
 *
 *
 * Sparkling Water can run in two modes. External cluster mode and internal cluster mode. When using external cluster
 * mode, it tries to connect to existing H2O cluster using the provided spark
 * configuration properties. In the case of internal cluster mode,it creates H2O cluster living in Spark - that means
 * that each Spark executor will have one h2o instance running in it.  This mode is not
 * recommended for big clusters and clusters where Spark executors are not stable.
 *
 * Cluster mode can be set using the spark configuration
 * property spark.ext.h2o.mode which can be set in script starting sparkling-water or
 * can be set in H2O configuration class H2OConf
 *
 */
public class JavaH2OContext {
/*
Note for developers: This class is not written in scala intentionally as we want to have static method getOrCreate on
the class itself and not on class generated from the object ( like H2OContext$). This way the functionality and API
remains the same as in H2OContext, but we need to write a few pass-through functions.

If we write this class in scala the Java users would have to call getOrCreate method on generated class ending with $
which is not nice.
*/
    transient private H2OContext hc;

    public H2OContext h2oContext(){
        return hc;
    }

    public SparkContext sparkContext(){
        return hc.sparkContext();
    }
    /**
     * Create new JavaH2OContext based on existing H2O Context
     *
     * @param hc H2O Context
     */
    private JavaH2OContext(H2OContext hc){
        this.hc = hc;
    }

    /**
     * Get Java H2O Context based on existing H2O Context
     * @param hc H2O Context
     * @return Java H2O Context
     */
    public static JavaH2OContext getFromExisting(H2OContext hc){
        return new JavaH2OContext(hc);
    }

    /**
     * Pass-through to H2OContext.asH2OFrame.  For API support only.
     * @param df data frame to pass for building an H2OFrame
     * @return a new H2O frame
     */
    public H2OFrame asH2OFrame(Dataset<Row> df){
        return hc.asH2OFrame(df);
    }

    /**
     * Pass-through to H2OContext.asH2OFrame.  For API support only.
     * @param df data frame to pass for building an H2OFrame
     * @param frameName name of the new frame
     * @return a new H2O frame
     */
    public H2OFrame asH2OFrame(Dataset<Row> df, String frameName){
        return hc.asH2OFrame(df, frameName);
    }


    /**
     * Create a new H2OFrame based on existing Frame referenced by its key.
     * @param s the key
     * @return a new H2O frame
     */
     public H2OFrame asH2OFrame(String s){
        return hc.asH2OFrame(s);
     }

    /**
     * Create a new H2OFrame based on existing Frame
     * @param fr the frame to be used
     * @return Java H2O Context
     */
    public H2OFrame asH2OFrame(Frame fr){
        return hc.asH2OFrame(fr);
    }

    /**
     * Convert given H2O frame into a Product RDD type
     * @param fr the frame to be used
     * @param <A> type of data being handled
     * @return a new RDD
     */
     public <A> JavaRDD<A> asRDD(H2OFrame fr){
        //TODO: Implement this conversion
       //return hc.asRDD(fr, (RDD<A>)JavaSparkContext.fakeClassTag())
        return null;
    }

    /**
     * Convert given H2O frame into DataFrame type
     * @param fr the frame to be used
     * @param sqlContext sql context to be used for creating a frame
     * @return a new data frame
     */
    public Dataset<Row> asDataFrame(Frame fr, SQLContext sqlContext){
        return asDataFrame(fr, true, sqlContext);
    }

    /** Convert given H2O frame into DataFrame type */
    public Dataset<Row> asDataFrame(Frame fr, boolean copyMetadata, SQLContext sqlContext){
        return hc.asDataFrame(fr, copyMetadata, sqlContext);
    }


    /** Convert given H2O frame into DataFrame type */
    public Dataset<Row> asDataFrame(String key, SQLContext sqlContext){
        return asDataFrame(key, true, sqlContext);
    }
    /** Convert given H2O frame into DataFrame type */
    public Dataset<Row> asDataFrame(String key, boolean copyMetadata, SQLContext sqlContext){
        return hc.asDataFrame(key, copyMetadata, sqlContext);
    }

    /** Pass-through to H2OContext.toH2OFrameKey.  For API support only.*/
    public Key<Frame> toH2OFrameKey(Dataset<Row> df){
        return hc.toH2OFrameKey(df);
    }

    /** Pass-through to H2OContext.toH2OFrameKey.  For API support only.*/
    public Key<Frame> toH2OFrameKey(Dataset<Row> df, Option<String> frameName){
        return hc.toH2OFrameKey(df, frameName);
    }

    /** Pass-through to H2OContext.toH2OFrameKey.  For API support only.*/
    public Key<Frame> toH2OFrameKey(Dataset<Row> df, String frameName){
        return hc.toH2OFrameKey(df, frameName);
    }

    /**
     * Get existing or create new JavaH2OContext based on provided H2O configuration. It searches the configuration
     * properties passed to Sparkling Water and based on them starts H2O Context. If the values are not found, the default
     * values are used in most of the cases. The default cluster mode is internal, ie. spark.ext.h2o.external.cluster.mode=false
     *
     * @param jsc Java Spark Context
     * @return Java H2O Context
     */
    public static JavaH2OContext getOrCreate(JavaSparkContext jsc){
        H2OConf conf = new H2OConf(jsc.sc());
        return getOrCreate(jsc, conf);
    }

    /**
     * Get existing or create new JavaH2OContext based on provided H2O configuration
     *
     * @param jsc Java Spark Context
     * @param conf H2O configuration
     * @return Java H2O Context
     */
    public static JavaH2OContext getOrCreate(JavaSparkContext jsc, H2OConf conf){
        return new JavaH2OContext(H2OContext.getOrCreate(jsc.sc(), conf));
    }


    public String toString(){
        return hc.toString();
    }

    public String h2oLocalClient(){
       return hc.h2oLocalClient();
    }

    public String h2oLocalClientIp(){
        return hc.h2oLocalClientIp();
    }

    public int h2oLocalClientPort(){
        return hc.h2oLocalClientPort();
    }

    public void stop(boolean stopSparkContext){
        hc.stop(stopSparkContext);
    }

    public void openFlow(){
        hc.openFlow();
    }
    /**
     * Return a copy of this JavaH2OContext's configuration. The configuration ''cannot'' be changed at runtime.
     */
    public H2OConf getConf(){
        return hc.getConf();
    }

    /** Conversion from RDD[String] to H2O's DataFrame */
    public H2OFrame asH2OFrameFromRDDString(JavaRDD<String> rdd, String frameName){
        return hc.asH2OFrame(SupportedRDD$.MODULE$.toH2OFrameFromRDDString(rdd.rdd()), Option.apply(frameName));
    }

    /** Conversion from RDD[Boolean] to H2O's DataFrame */
    public H2OFrame asH2OFrameFromRDDBool(JavaRDD<Boolean> rdd, String frameName){

        return hc.asH2OFrame(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaBool(rdd.rdd()), Option.apply(frameName));
    }

    /** Conversion from RDD[Integer] to H2O's DataFrame */
    public H2OFrame asH2OFrameFromRDDInt(JavaRDD<Integer> rdd, String frameName){
        return hc.asH2OFrame(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaInt(rdd.rdd()), Option.apply(frameName));
    }

    /** Conversion from RDD[Byte] to H2O's DataFrame */
    public H2OFrame asH2OFrameFromRDDByte(JavaRDD<Byte> rdd, String frameName){

        return hc.asH2OFrame(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaByte(rdd.rdd()), Option.apply(frameName));
    }

    /** Conversion from RDD[Short] to H2O's DataFrame */
    public H2OFrame asH2OFrameFromRDDShort(JavaRDD<Short> rdd, String frameName){

        return hc.asH2OFrame(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaShort(rdd.rdd()), Option.apply(frameName));
    }

    /** Conversion from RDD[Float] to H2O's DataFrame */
    public H2OFrame asH2OFrameFromRDDFloat(JavaRDD<Float> rdd, String frameName){

        return hc.asH2OFrame(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaFloat(rdd.rdd()), Option.apply(frameName));
    }

    /** Conversion from RDD[Double] to H2O's DataFrame */
    public H2OFrame asH2OFrameFromRDDDouble(JavaRDD<Double> rdd, String frameName){
        return hc.asH2OFrame(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaDouble(rdd.rdd()), Option.apply(frameName));
    }

    /** Conversion from RDD[Double] to H2O's DataFrame
     * This method is used by the python client since even though the rdd is of type double,
     * some of the elements are actually integers. We need to convert all types to double in order to not break the
     * backend
     * */
    public H2OFrame asH2OFrameFromPythonRDDDouble(JavaRDD<Number> rdd, String frameName){
        JavaRDD<Double> casted = rdd.map(new RDDDoubleConversionFunc());
        return asH2OFrameFromRDDDouble(casted, frameName);
    }


    /** Conversion from RDD[Long] to H2O's DataFrame */
    public H2OFrame asH2OFrameFromRDDLong(JavaRDD<Long> rdd, String frameName){
        return hc.asH2OFrame(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaLong(rdd.rdd()), Option.apply(frameName));
    }

    /** Conversion from RDD[Long] to H2O's DataFrame
     * This method is used by the python client since even though the rdd is of type long,
     * some of the elements are actually integers. We need to convert all types to long in order to not break the
     * backend
     * */
    public H2OFrame asH2OFrameFromPythonRDDLong(JavaRDD<Number> rdd, String frameName){
        JavaRDD<Long> casted = rdd.map(new RDDLongConversionFunc());
        return asH2OFrameFromRDDLong(casted, frameName);
    }

    /** Conversion from RDD[LabeledPoint] to H2O's DataFrame */
    public H2OFrame asH2OFrameFromRDDLabeledPoint(JavaRDD<LabeledPoint> rdd, String frameName){
        return hc.asH2OFrame(SupportedRDD$.MODULE$.toH2OFrameFromRDDLabeledPoint(rdd.rdd()), Option.apply(frameName));
    }

    /** Conversion from RDD[java.sql.TimeStamp] to H2O's DataFrame */
    public H2OFrame asH2OFrameFromRDDTimeStamp(JavaRDD<java.sql.Timestamp> rdd, String frameName){
        return hc.asH2OFrame(SupportedRDD$.MODULE$.toH2OFrameFromRDDTimeStamp(rdd.rdd()), Option.apply(frameName));
    }
    /** Returns key of the H2O's DataFrame conversed from RDD[String]*/
    @SuppressWarnings("unchecked")
    public Key<Frame> asH2OFrameFromRDDStringKey(JavaRDD<String> rdd, String frameName){
        return (Key<Frame>) hc.toH2OFrameKey(SupportedRDD$.MODULE$.toH2OFrameFromRDDString(rdd.rdd()), Option.apply(frameName));
    }

    /** Returns key of the H2O's DataFrame conversed from RDD[Boolean]*/
    @SuppressWarnings("unchecked")
    public Key<Frame> asH2OFrameFromRDDBoolKey(JavaRDD<Boolean> rdd, String frameName){
        return (Key<Frame>) hc.toH2OFrameKey(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaBool(rdd.rdd()), Option.apply(frameName));
    }

    /** Returns key of the H2O's DataFrame conversed from RDD[Integer]*/
    @SuppressWarnings("unchecked")
    public Key<Frame> asH2OFrameFromRDDIntKey(JavaRDD<Integer> rdd, String frameName){
        return (Key<Frame>) hc.toH2OFrameKey(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaInt(rdd.rdd()), Option.apply(frameName));
    }

    /** Returns key of the H2O's DataFrame conversed from RDD[Byte]*/
    @SuppressWarnings("unchecked")
    public Key<Frame> asH2OFrameFromRDDByteKey(JavaRDD<Byte> rdd, String frameName){
        return (Key<Frame>) hc.toH2OFrameKey(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaByte(rdd.rdd()), Option.apply(frameName));
    }

    /** Returns key of the H2O's DataFrame conversed from RDD[Short]*/
    @SuppressWarnings("unchecked")
    public Key<Frame> asH2OFrameFromRDDShortKey(JavaRDD<Short> rdd, String frameName){
        return (Key<Frame>) hc.toH2OFrameKey(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaShort(rdd.rdd()), Option.apply(frameName));
    }

    /** Returns key of the H2O's DataFrame conversed from RDD[Float]*/
    @SuppressWarnings("unchecked")
    public Key<Frame> asH2OFrameFromRDDFloatKey(JavaRDD<Float> rdd, String frameName){
        return (Key<Frame>) hc.toH2OFrameKey(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaFloat(rdd.rdd()), Option.apply(frameName));
    }

    /** Returns key of the H2O's DataFrame conversed from RDD[Double]*/
    @SuppressWarnings("unchecked")
    public Key<Frame> asH2OFrameFromRDDDoubleKey(JavaRDD<Double> rdd, String frameName) {
        return (Key<Frame>) hc.toH2OFrameKey(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaDouble(rdd.rdd()), Option.apply(frameName));
    }

    /** Returns key of the H2O's DataFrame conversed from RDD[Long]*/
    @SuppressWarnings("unchecked")
    public Key<Frame> asH2OFrameFromRDDLongKey(JavaRDD<Long> rdd, String frameName){
        return (Key<Frame>) hc.toH2OFrameKey(SupportedRDD$.MODULE$.toH2OFrameFromRDDJavaLong(rdd.rdd()), Option.apply(frameName));
    }

    /** Returns key of the H2O's DataFrame conversed from RDD[LabeledPoint]*/
    @SuppressWarnings("unchecked")
    public Key<Frame> asH2OFrameFromRDDLabeledPointKey(JavaRDD<LabeledPoint> rdd, String frameName){
        return (Key<Frame>) hc.toH2OFrameKey(SupportedRDD$.MODULE$.toH2OFrameFromRDDLabeledPoint(rdd.rdd()), Option.apply(frameName));
    }

    /** Returns key of the H2O's DataFrame conversed from RDD[java.sql.Timestamp]*/
    @SuppressWarnings("unchecked")
    public Key<Frame> asH2OFrameFromRDDTimeStampKey(JavaRDD<java.sql.Timestamp> rdd, String frameName){
        return (Key<Frame>) hc.toH2OFrameKey(SupportedRDD$.MODULE$.toH2OFrameFromRDDTimeStamp(rdd.rdd()), Option.apply(frameName));
    }
}
