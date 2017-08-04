#!/usr/bin/groovy

pipeline{

    agent { label 'mr-0xd3' }

    parameters {

        booleanParam(name: 'runUnitTests', defaultValue: true, description: 'Run unit and pyunit tests')
        booleanParam(name: 'runLocalIntegTests', defaultValue: true, description: 'Run local integration tests')
        booleanParam(name: 'runScriptTests', defaultValue: true, description: 'Run script tests')
        booleanParam(name: 'runIntegTests', defaultValue: true, description: 'Run integration tests')
        booleanParam(name: 'runPySparklingIntegTests', defaultValue: true, description: 'Run pySparkling integration tests')

        choice(
            choices: '2.1.0\n2.0.2\n2.0.1\n2.0.0\n1.6.3\n1.6.2\n1.6.1\n1.6.0',
            description: 'Version of Spark used for testing.',
            name: 'sparkVersion')

        string(name: 'sparklingTestEnv', defaultValue: 'yarn', description: 'Sparkling water test profile (default yarn)')

        choice(
                choices: 'internal\nexternal',
                description: 'Sparkling Water backend mode.',
                name: 'backendMode')

        string(name: 'hdpVersion', defaultValue: 'current', description: 'HDP version to pass to Spark configuration - for example, 2.2.0.0-2041, or 2.6.0.2.2, or current. When running external tests on yarn, the current will not do since it is not automatically expanded -> so please set 2.2.6.3-1')
        booleanParam(name: 'startH2OClusterOnYarn', defaultValue: false, description: "In case of external backend, determines whether the external H2O cluster is started on yarn or locally")
        string(name: 'driverHadoopVersion', defaultValue: 'hdp2.2', description: 'Hadoop version for which H2O driver will be obtained')

    }

    environment {
        HADOOP_VERSION  = "2.6" 
        SPARK           = "spark-${params.sparkVersion}-bin-hadoop${HADOOP_VERSION}"
        SPARK_HOME      = "${env.WORKSPACE}/spark"
        HADOOP_CONF_DIR = "/etc/hadoop/conf"
        MASTER          = "yarn-client"
        H2O_PYTHON_WHEEL="${env.WORKSPACE}/private/h2o.whl"
        H2O_EXTENDED_JAR="${env.WORKSPACE}/assembly-h2o/private/"
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
                    ${env.WORKSPACE}/gradlew test -x integTest -PbackendMode=${params.backendMode} -PstartH2OClusterOnYarn=${params.startH2OClusterOnYarn}
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
                    ${env.WORKSPACE}/gradlew integTest -PsparkHome=${env.SPARK_HOME} -PbackendMode=${params.backendMode} -PstartH2OClusterOnYarn=${params.startH2OClusterOnYarn}
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

             steps {
                    sh """
                    # Run scripts tests
                    ${env.WORKSPACE}/gradlew scriptTest -PbackendMode=${params.backendMode} -PstartH2OClusterOnYarn=${params.startH2OClusterOnYarn}
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

        stage('QA: Integration tests') {
            when {
                expression { params.runIntegTests == true }
            }
            steps {
                sh """
                    ${env.WORKSPACE}/gradlew integTest -PbackendMode=${params.backendMode} -PstartH2OClusterOnYarn=${params.startH2OClusterOnYarn} -PsparklingTestEnv=${params.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
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
                expression { runPySparklingIntegTests == true }
            }
            steps {
                sh """
                    ${env.WORKSPACE}/gradlew integTestPython -PbackendMode=${params.backendMode} -PstartH2OClusterOnYarn=${params.startH2OClusterOnYarn} -PsparklingTestEnv=${params.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check
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


// Def sections

def success(message) {

    sh 'echo "Test ran successful"'

    step([$class: 'GitHubCommitStatusSetter',
        contextSource: [$class: 'ManuallyEnteredCommitContextSource',
        context: 'h2o-ops'],
        statusResultSource: [$class: 'ConditionalStatusResultSource',
        results: [[$class: 'AnyBuildResult',
        state: 'SUCCESS',
        message: message ]]]])

}

def failure(message) {

        sh 'echo "Test failed "'
        step([$class: 'GitHubCommitStatusSetter',
            contextSource: [$class: 'ManuallyEnteredCommitContextSource',
            context: 'h2o-ops'],
            statusResultSource: [$class: 'ConditionalStatusResultSource',
            results: [[$class: 'AnyBuildResult',
            message: message,
            state: 'FAILURE']]]])
}

def testReport(reportDirectory, title) {
    publishHTML target: [
        allowMissing: false,
        alwaysLinkToLastBuild: true,
        keepAll: true,
        reportDir: reportDirectory,
        reportFiles: 'index.html',
        reportName: title
    ]
}

def arch(list) {
    archiveArtifacts artifacts: list, allowEmptyArchive: true
}

