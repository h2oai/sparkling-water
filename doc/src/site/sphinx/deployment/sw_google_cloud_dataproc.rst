Running Sparkling Water on Google Cloud Dataproc
------------------------------------------------

This section describes how to run Sparkling Water on `Google Cloud Dataproc <https://cloud.google.com/dataproc/docs/concepts/overview>`__. 

Perform the following steps to start Sparkling Water ``H2OContext`` on Cloud Dataproc. 

**Note**: Be sure to allocate enough resources to handle your data. As a minimum recommendation, you should allocate 4 times memory of the size of your data in H2O.

1. Install the `Google SDK <https://cloud.google.com/sdk/gcloud/>`__.

2. Install `Daisy <https://googlecloudplatform.github.io/compute-image-tools/daisy-installation-usage.html>`__. The path for Daisy will be required in a later step.

3. After Daisy is installed, download and save Google's `generate_custom_image.py <https://github.com/GoogleCloudPlatform/cloud-dataproc/blob/master/custom-images/generate_custom_image.py>`__. This custom image will be referenced in a later step.

4. Copy and save the following customization script. This will be required in a later step.

 ::

	#!/bin/bash

	apt-get update
	apt-get install -y python-dev python-pip jq

	cd /usr/lib/
	wget http://h2o-release.s3.amazonaws.com/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_VERSION/sparkling-water-SUBST_SW_VERSION.zip
	unzip sparkling-water-SUBST_SW_VERSION.zip

	pip install pip==9.0.3
	pip install requests==2.18.4
	pip install tabulate
	pip install future
	pip install colorama
	pip install scikit-learn==0.19.1
	pip install https://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/Python/h2o-SUBST_H2O_VERSION-py2.py3-none-any.whl
	pip install --upgrade google-cloud-bigquery
	pip install --upgrade google-cloud-storage

	pip install h2o_pysparkling_SUBST_SPARK_MAJOR_VERSION


5. Create a Dataproc Custom Image as defined `here <https://cloud.google.com/dataproc/docs/guides/dataproc-images>`__. Note that this references the Daisy path, the custom image name, and the customization script from previous steps. The code will be similar to the following:

 ::

   python generate_custom_image.py \
    --image-name <custom_image_name> \
    --dataproc-version <Cloud Dataproc version (example: "1.2.22")> \
    --customization-script <local path to your custom script> \
    --daisy-path <local path to the downloaded daisy binary> \
    --zone <Compute Engine zone to use for the location of the temporary VM> \
    --gcs-bucket <URI (gs://bucket-name) of a Cloud Storage bucket in your project> \
    [--no-smoke-test: <"true" or "false" (default: "false")>

6. Copy and save the following initialization script into a GCS bucket that you have access to. This will be required when you create your cluster.

 ::

	#!/bin/bash

	METADATA_ROOT='http://metadata/computeMetadata/v1/instance/attributes'

	CLUSTER_NAME=$(curl -f -s -H Metadata-Flavor:Google \
	${METADATA_ROOT}/dataproc-cluster-name)

	cat << EOF > /usr/local/bin/await_cluster_and_run_command.sh
	#!/bin/bash
	# Helper to get current cluster state.
	function get_cluster_state() {
		echo \$(gcloud dataproc clusters describe ${CLUSTER_NAME} | \
	  	grep -A 1 "^status:" | grep "state:" | cut -d ':' -f 2)
	}
	# Helper which waits for RUNNING state before issuing the command.
	function await_and_submit() {
		local cluster_state=\$(get_cluster_state)
		echo "Current cluster state: \${cluster_state}"
		while [[ "\${cluster_state}" != 'RUNNING' ]]; do
		echo "Sleeping to await cluster health..."
		sleep 5
		local cluster_state=\$(get_cluster_state)
		if [[ "\${cluster_state}" == 'ERROR' ]]; then
		  echo "Giving up due to cluster state '\${cluster_state}'"
		  exit 1
		fi
		done

		echo "Changing Spark Configurations"
		sudo sed -i 's/spark.dynamicAllocation.enabled true/spark.dynamicAllocation.enabled false/g' /usr/lib/spark/conf/spark-defaults.conf
		sudo sed -i 's/spark.executor.instances 10000/# spark.executor.instances 10000/g' /usr/lib/spark/conf/spark-defaults.conf
		sudo sed -i 's/spark.executor.cores.*/# removing unnecessary limits to executor cores/g' /usr/lib/spark/conf/spark-defaults.conf
		sudo sed -i 's/^spark.executor.memory.*/# removing unnecessary limits to executor memory/g' /usr/lib/spark/conf/spark-defaults.conf
		sudo echo "spark.executor.instances $(gcloud dataproc clusters describe ${CLUSTER_NAME} | grep "numInstances:" | tail -1 | sed "s/.*numInstances: //g")" >> /usr/lib/spark/conf/spark-defaults.conf
		sudo echo "spark.executor.cores $(gcloud compute machine-types describe $(gcloud dataproc clusters describe ${CLUSTER_NAME} | grep "machineTypeUri" | tail -1 | sed 's/.*machineTypeUri: //g') | grep "guestCpus" | sed 's/guestCpus: //g')" >> /usr/lib/spark/conf/spark-defaults.conf
		sudo echo "spark.executor.memory $(($(gcloud compute machine-types describe $(gcloud dataproc clusters describe h2o-dataproc | grep "machineTypeUri" | tail -1 | sed 's/.*machineTypeUri: //g') | grep "memoryMb:" | sed 's/memoryMb: //g') * 65 / 100))m" >> /usr/lib/spark/conf/spark-defaults.conf
		echo "Successfully Changed spark-defaults.conf"

		cat /usr/lib/spark/conf/spark-defaults.conf
	}

	await_and_submit
	EOF

	chmod 750 /usr/local/bin/await_cluster_and_run_command.sh
	nohup /usr/local/bin/await_cluster_and_run_command.sh &>> \
	/var/log/master-post-init.log &

7. After the image is created and the script is saved, create the cluster as defined `here <https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create>`__ using the script created above. The only required flags are ``image`` and ``--initialization-actions``. 

 ::

  gcloud dataproc clusters create sparklingwaterdataproc \
   --image=<myswdataprocimage> \
   --initialization-actions=gs://<bucket>/<initialization_script.sh> 

Upon successful completion, you will have a Dataproc running Sparkling Water. You can run jobs now, for example:

::

  gcloud dataproc jobs submit pyspark \
    --cluster cluster-name --region region \
    sample-script.py 


**Note**: Dataproc does not automatically enable Spark logs. Refer to the following Stackoverflow answers:

- `Google Dataproc Pyspark Properties <https://stackoverflow.com/questions/47342132/where-are-the-individual-dataproc-spark-logs>`__
- `Where are the individual dataproc spark logs? <https://stackoverflow.com/questions/48779612/google-dataproc-pyspark-properties>`__

Sample Script for Sparkling Water Job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Below is a sample script for running a Sparkling Water job. Edit the arguments to match your bucket and GCP setup.

.. code:: python

	import h2o
	from h2o.automl import H2OAutoML
	from pyspark.sql import SparkSession
	from pysparkling import *

	spark = SparkSession.builder.appName("SparklingWaterApp").getOrCreate()
	hc = H2OContext.getOrCreate(spark)

	bucket = "h2o-bq-large-dataset"
	train_path = "demos/cc_train.csv"
	test_path = "demos/cc_test.csv"
	y = "DEFAULT_PAYMENT_NEXT_MONTH"
	is_classification = True

	drop_cols = []
	aml_args = {"max_runtime_secs": 120}

	train_data = spark.read\
	                  .options(header='true', inferSchema='true')\
	                  .csv("gs://{}/{}".format(bucket, train_path))
	test_data = spark.read\
	                 .options(header='true', inferSchema='true')\
	                 .csv("gs://{}/{}".format(bucket, test_path))

	print("CREATING H2O FRAME")
	training_frame = hc.as_h2o_frame(train_data)
	test_frame = hc.as_h2o_frame(test_data)

	x = training_frame.columns
	x.remove(y)

	for col in drop_cols:
	    x.remove(col)

	if is_classification:
	    training_frame[y] = training_frame[y].asfactor()
	else:
	    print("REGRESSION: Not setting target column as factor")

	print("TRAINING H2OAUTOML")
	aml = H2OAutoML(**aml_args)
	aml.train(x=x, y=y, training_frame=training_frame)

	print(aml.leaderboard)

	print('SUCCESS')

