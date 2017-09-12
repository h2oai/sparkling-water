#!/usr/bin/groovy

@Library('test-shared-library') _

pipeline{

    // Use given machines to run pipeline
    agent { label 'dX-hadoop' }

    // Setup job options
    options {
        ansiColor('xterm')
        timestamps()
        timeout(time: 120, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }

    // Job parameters
    parameters {
        choice(
                choices: '1.6.3\n1.6.2\n1.6.1\n1.6.0',
                description: 'Version of Spark used for testing.',
                name: 'sparkVersion')

        booleanParam(name: 'runUnitTests', defaultValue: true, description: 'Run unit and pyunit tests')
        booleanParam(name: 'runLocalIntegTests', defaultValue: true, description: 'Run local integration tests')
        booleanParam(name: 'runScriptTests', defaultValue: true, description: 'Run script tests')
        booleanParam(name: 'runIntegTests', defaultValue: true, description: 'Run integration tests')
        booleanParam(name: 'runPySparklingIntegTests', defaultValue: true, description: 'Run pySparkling integration tests')


        choice(
                choices: 'yarn\nstandalone\nlocal',
                description: 'Sparkling water test profile',
                name: 'sparklingTestEnv')

        choice(
                choices: 'internal\nexternal',
                description: 'Sparkling Water backend mode.',
                name: 'backendMode')

        choice(
                choices: 'manual\nauto',
                description: 'Start mode of H2O cluster in external backend',
                name: 'externalBackendStartMode')

        choice(
                choices: 'hdp2.2\nhdp2.3\nhdp2.4\nhdp2.5\nhdp2.6\nmapr4.0\nmapr5.0\nmapr5.1\nmapr5.2\niop4.2',
                description: 'Hadoop version for which H2O driver is obtained',
                name: 'driverHadoopVersion')

        string(name: 'hdpVersion', defaultValue: 'current', description: 'HDP version to pass to Spark configuration - for example, 2.2.0.0-2041, or 2.6.0.2.2, or current. When running external tests on yarn, the current will not do since it is not automatically expanded -> so please set 2.2.6.3-1')

    }

    environment {
        HADOOP_VERSION  = "2.6" 
        SPARK           = "spark-${params.sparkVersion}-bin-hadoop${HADOOP_VERSION}"
        SPARK_HOME      = "${env.WORKSPACE}/spark"
        HADOOP_CONF_DIR = "/etc/hadoop/conf"
        MASTER          = "yarn-client"
        H2O_PYTHON_WHEEL= "${env.WORKSPACE}/private/h2o.whl"
        H2O_EXTENDED_JAR= "${env.WORKSPACE}/assembly-h2o/private/extended/h2odriver-extended.jar"
        JAVA_HOME       = "${JAVA_HOME_8}"
        PATH            = "${JAVA_HOME}/bin:${PATH}"
    }

    stages {

        stage('Git Checkout and Preparation'){

            steps{
                checkout scm
                sh """
                if [ ! -d "${env.SPARK_HOME}" ]; then
                        wget -q "http://d3kbcqa49mib13.cloudfront.net/${env.SPARK}.tgz"
                        mkdir -p "${env.SPARK_HOME}"
                        tar zxvf ${env.SPARK}.tgz -C "${env.SPARK_HOME}" --strip-components 1
                        rm -rf ${env.SPARK}.tgz
                fi
                """

                // Spark work directory cleanup
                dir("${env.SPARK_HOME}/work") {
                    deleteDir()
                }
            }
        }

        stage('QA: Prepare Environment and Data') {
            steps {
                sh """
                    # Setup 
                    echo "spark.driver.extraJavaOptions -Dhdp.version="${params.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                    echo "spark.yarn.am.extraJavaOptions -Dhdp.version="${params.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                    echo "spark.executor.extraJavaOptions -Dhdp.version="${params.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf

                    echo "-Dhdp.version="${params.hdpVersion}"" >> ${env.SPARK_HOME}/conf/java-opts
                   """

                sh """
                    mkdir -p ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-07.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-07.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-08.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-08.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-09.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-09.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-10.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-10.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-11.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-11.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-12.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-12.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/31081_New_York_City__Hourly_2013.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/31081_New_York_City__Hourly_2013.csv
                   """

                sh """
                    # Download h2o-python client, save it in private directory
                    # and export variable H2O_PYTHON_WHEEL driving building of pysparkling package
                    mkdir -p ${env.WORKSPACE}/private/
                    curl -s `./gradlew -q printH2OWheelPackage` > ${env.WORKSPACE}/private/h2o.whl
                    ./gradlew -q extendJar -PdownloadH2O=${params.driverHadoopVersion}
                   """
            }
        }

        stage('QA: Build and Lint') {
            steps {
                sh  """
                    # Build
                    ${env.WORKSPACE}/gradlew clean build -x check scalaStyle
                    """
            }
        }

        stage('QA: Unit Tests') {
            when {
                expression { params.runUnitTests == true }
            }
            steps {
                sh  """
                    # Build, run regular tests
                    ${env.WORKSPACE}/gradlew test -x integTest -PbackendMode=${params.backendMode} -PexternalBackendStartMode=${params.externalBackendStartMode}
                    """
            }

            post {
                always {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr, **/build/**/*log*, py/build/py_*_report.txt, **/build/reports/'
                    junit 'core/build/test-results/test/*.xml'
                    testReport 'core/build/reports/tests/test', 'Core Unit tests'
                }
            }
        }

        stage('QA: Local Integration Tests') {

            when {
                expression { params.runLocalIntegTests == true }
            }

            steps {
                sh  """
                    # Build, run regular tests
                    ${env.WORKSPACE}/gradlew integTest -PsparkHome=${env.SPARK_HOME} -PbackendMode=${params.backendMode} -PexternalBackendStartMode=${params.externalBackendStartMode}
                    """
            }

            post {
                always {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                    junit 'examples/build/test-results/integTest/*.xml'
                    testReport 'core/build/reports/tests/integTest', 'Local Core Integration tests'
                    testReport 'examples/build/reports/tests/integTest', 'Local Examples Integration tests'
                }
            }
        }

        stage('QA: Script Tests') {
            when {
                expression { params.runScriptTests == true }
            }
            steps {
                sh """
                # Run scripts tests
                ${env.WORKSPACE}/gradlew scriptTest -PbackendMode=${params.backendMode} -PexternalBackendStartMode=${params.externalBackendStartMode}
                """
		    }
			post {
				always {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                    junit 'examples/build/test-results/scriptsTest/*.xml'
                    testReport 'examples/build/reports/tests/scriptsTest', 'Examples Script Tests'
				}
			}
        }

        stage('QA: Integration Tests') {
            when {
                expression { params.runIntegTests == true }
            }
            steps {
                sh """
                    ${env.WORKSPACE}/gradlew integTest -PbackendMode=${params.backendMode} -PexternalBackendStartMode=${params.externalBackendStartMode} -PsparklingTestEnv=${params.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
                     #  echo 'Archiving artifacts after Integration test'
                 """
            }
            post {
				always {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                    junit 'examples/build/test-results/integTest/*.xml'
					testReport 'examples/build/reports/tests/integTest', "${params.backendMode} Examples Integration tests"

				}
			}
        }

        stage('QA: PySparkling Integration Tests') {
            when {
                expression { params.runPySparklingIntegTests == true }
            }
            steps {
                sh  """
                    ${env.WORKSPACE}/gradlew integTestPython -PbackendMode=${params.backendMode} -PexternalBackendStartMode=${params.externalBackendStartMode} -PsparklingTestEnv=${params.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check
                    #  echo 'Archiving artifacts after PySparkling Integration test'
                    """
            }
            post {
                always {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                }
            }
        }
    }
}