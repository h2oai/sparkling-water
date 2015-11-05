#!/usr/bin/env bash

# Current dir
TOPDIR=$(cd `dirname $0`/.. &&  pwd)
source $TOPDIR/bin/sparkling-env.sh
# Verify there is Spark installation
checkSparkHome

if [[ -z "$MASTER" ]]; then
    echo "Please setup MASTER variable!"
    exit -1
fi

SPARKLING_HOME=$TOPDIR

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

export PYTHONPATH=$H2O_HOME/h2o-py:$PYTHONPATH
export PYSPARK_SUBMIT_ARGS="--master $MASTER"

echo "---------------------------------------"
echo "Using SPARK_HOME: $SPARK_HOME          "
echo "Using MASTER: $MASTER                  "
echo "Using H2O_HOME: $H2O_HOME              "
echo "---------------------------------------"

JUPYTER_CONFIG_DIR=$custom_conf_dir IPYTHON_OPTS="notebook" $TOPDIR/bin/pysparkling "$@"

