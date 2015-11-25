# pySparkling

## Goal
Provide transparent user experience of using Sparkling Water from Python.
It includes:
  - support creation of H2OContext
  - support data transfers - from H2OFrame to DataFrame/RDD and back
  - transparent use of H2OFrames and RDDs

## Usage

Command to launch pyspark with Sparkling Water:
 ```
PYSPARK_PYTHON=ipython $SPARK_HOME/bin/pyspark --packages ai.h2o:sparkling-water-core_2.10:1.5.2,ai.h2o:sparkling-water-examples_2.10:1.5.2
```

Command to launch Python script `script.py` via `spark-submit`:
```
$SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-core_2.10:1.5.2,ai.h2o:sparkling-water-examples_2.10:1.5.2 script.py
```

Creating H2O Context:
```
hc = H2OContext(sc)
```


Testing - in sparkling-water/py directory:
```
import sys
sys.path.append(".")
import pysparkling.context
hc = pysparkling.context.H2OContext(sc)
hc.start()
```

## Technical details

Use Py4J bundled with PySpark to access JVM classes

```
from py4j.java_gateway import java_import
java_import(sc._jvm, "org.apache.spark.h2o.*")
jvm = sc._jvm
gw = sc._gateway
hc_klazz = jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass("org.apache.spark.h2o.H2OContext")
ctor_def = gw.new_array(jvm.Class, 1)
ctor_def[0] = sc._jsc.getClass()
hc_ctor = hc_klazz.getConstructor(ctor_def)
ctor_params = gw.new_array(jvm.Object, 1)
ctor_params[0] = sc._jsc
hc = hc_ctor.newInstance(ctor_params)
hc.start()

```

# Running and debugging

## From Python notebook
Start script:
 
```
#!/usr/bin/env bash
export PYTHONPATH=$H2O_HOME/h2o-py:$SPARKLING_HOME/py:$PYTHONPATH
export SPARK_CLASSPATH=$SPARK_CLASSPATH:$SPARKLING_HOME/assembly/build/libs/sparkling-water-assembly-0.2.17-SNAPSHOT-all.jar
echo $SPARK_CLASSPATH
IPYTHON_OPTS="notebook" $SPARK_HOME/bin/pyspark
```

where:
  - `H2O_HOME` points to H2O project which contains right python package
  - `SPARKLING_HOME`  points to Sparkling project git repository
  - before run, Sparkling Water has to be build: `./gradlew build -x test`

# Testing

## Development testing
```
  export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:./:$PYTHONPATH
  export SPARK_CLASSPATH=./sparkling-water/assembly/build/libs/sparkling-water-assembly-0.2.17-SNAPSHOT-all.jar
  python -m unittest pysparkling/tests.py
```
## Testing from Gradle
Needs:
  - SPARK_HOME
  - H2O_HOME (dev, downloadeded) - you can test against different H2O versions
  - python 
  - python packages for H2O

## Notes
For Python applications, simply pass a .py file in the place of <application-jar> instead of a JAR, and add Python .zip, .egg or .py files to the search path with --py-files.

## Problems

Spark Issues
  - https://issues.apache.org/jira/browse/SPARK-5185 - `--jars` packages are not appended to driver path
     * Solution: `sc._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass("org.apache.spark.h2o.H2OContext").newInstance()`
     * ```
       We've been setting SPARK_SUBMIT_CLASSPATH as a workaround to this issue, but as of https://github.com/apache/spark/commit/517975d89d40a77c7186f488547eed11f79c1e97 this variable no longer exists. We're now setting SPARK_CLASSPATH as a workaround.
       ```

  Related issue:
    - https://issues.apache.org/jira/browse/SPARK-6047


 # Neat
   - https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/DynamicClassLoader.java
