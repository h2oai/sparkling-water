#!/usr/bin/env bash

if [ ! -d "$SPARK_HOME" ]; then
    echo "Please setup SPARK_HOME variable to your Spark installation!"
    exit -1
  fi

if [ ! -d "$SPARKLING_HOME" ]; then
    echo "Please setup SPARKLING_HOME variable to your sparkling water installation!"
    exit -1
  fi

if [[ -z "$MASTER" ]]; then
    echo "Please setup MASTER variable!"
    exit -1
 fi

# check if the custom jupyter confif file exist and generate it if it does not exist
custom_conf_dir="$SPARKLING_HOME/jupyter_custom_config"
custom_conf_file="$custom_conf_dir/jupyter_notebook_config.py"
if [ ! -d "$custom_conf_dir" ]; then
	JUPYTER_CONFIG_DIR=$custom_conf_dir jupyter notebook --generate-config
	#edit the custom config file
	sed -i 's/# c.NotebookApp.port = 8888/c.NotebookApp.port = 42424/' $custom_conf_file
	# edit the default directory
	sed -i "s:# c.NotebookApp.notebook_dir = u'':c.NotebookApp.notebook_dir = u'py/examples/notebooks':" $custom_conf_file

fi

export PYTHONPATH=$H2O_HOME/h2o-py:$SPARKLING_HOME/py:$PYTHONPATH
export SPARK_CLASSPATH=$SPARK_CLASSPATH:$SPARKLING_HOME/assembly/build/libs/sparkling-water-assembly-0.2.17-SNAPSHOT-all.jar
export PYSPARK_SUBMIT_ARGS="--master $MASTER"

echo "---------------------------------------"
echo "Using SPARK_HOME: $SPARK_HOME          "
echo "Using MASTER: $MASTER                  "
echo "Using H2O_HOME: $H2O_HOME              "
echo "USIGN SPARK_CLASSPATH: $SPARK_CLASSPATH"
echo "---------------------------------------"

JUPYTER_CONFIG_DIR=$custom_conf_dir IPYTHON_OPTS="notebook" $SPARK_HOME/bin/pyspark

