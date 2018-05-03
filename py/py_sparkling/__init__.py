##
# The purpose of this package is to enable import and export of pipelines containing H2O algorithms in PySparkling.
# The problem is that Spark expects this mapping between Scala and Python -> org.apache.spark -> pyspark and vice versa.
# So for example, since we have package named pysparkling, it will get renamed to pyorg.apache.sparkling and it Spark
# will try to look up this class using py4j. This is solved by creating artificial helper package which does not contain
# pyspark substring
##
