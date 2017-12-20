package org.apache.spark.ml.h2o.models;

/**
 * Helper class allowing us to call H2OMOJOModel.createFromMojo defined in Scala via Py4j
 */
public class JavaH2OMOJOModelHelper {

    public static H2OMOJOModel createFromMojo(String path){
        return H2OMOJOModel$.MODULE$.createFromMojo(path);
    }

}
