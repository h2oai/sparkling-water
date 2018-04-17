package org.apache.spark.ml.h2o.models;

/**
 * Helper class allowing us to call H2OMOJOPipelineModel.createFromMojo defined in Scala via Py4j
 */
public class JavaH2OMOJOPipelineModelHelper {

  public static H2OMOJOPipelineModel createFromMojo(String path){
    return H2OMOJOPipelineModel$.MODULE$.createFromMojo(path);
  }

}
