#!/usr/bin/groovy

@Library('test-shared-library') _

pipeline{

    agent { label 'mr-0xd1 || mr-0xd2 || mr-0xd3 || mr-0xd4 || mr-0xd5 || mr-0xd7 || mr-0xd8 || mr-0xd9 || mr-0xd10' }

    options {
        timeout(time: 120, unit: 'MINUTES')
    }

    parameters {
        string(name: 'branchName', defaultValue: 'master', description: 'Test given branch on top of YARN.')
        string(name: 'hdpVersion', defaultValue: 'current', description: 'HDP version to pass to Spark configuration - for example, 2.2.0.0-2041, or 2.6.0.2.2, or current. When running external tests on yarn, the current will not do since it is not automatically expanded -> so please set 2.2.6.3-1')

        booleanParam(name: 'runBuildTests', defaultValue: true, description: 'Run build tests - junit and local integration tests')
        booleanParam(name: 'runScriptTests', defaultValue: true, description: 'Run script tests')
        booleanParam(name: 'runIntegTests', defaultValue: true, description: 'Run integration tests')
        booleanParam(name: 'startH2OClusterOnYarn', defaultValue: false)
        booleanParam(name: 'runPySparklingIntegTests', defaultValue: false, description: 'Run pySparkling integration tests')

        string(name: 'sparkVersion', defaultValue: '2.1.0', description: 'Version of Spark used for testing')
        string(name: 'sparklingTestEnv', defaultValue: 'yarn', description: 'Sparkling water test profile (default yarn)')
        string(name: 'backendMode', defaultValue: 'internal', description: '')
        string(name: 'driverHadoopVersion', defaultValue: 'hdp2.2', description: 'Hadoop version for which h2o driver will be obtained')

        string(name: 'artifactDirectory', defaultValue: 'artifacts', description: 'artifact directory')
        string(name: 'buildNumber', defaultValue: '99',description: 'Build number')
        booleanParam(name: 'nightlyBuild', defaultValue: false, description: 'Upload the artifacts if the build is nighlty')
    }

    environment {
        HADOOP_VERSION  = "2.6" 
        SPARK_HOME      = "${env.WORKSPACE}/spark-${params.sparkVersion}-bin-hadoop${HADOOP_VERSION}"
        HADOOP_CONF_DIR = "/etc/hadoop/conf"
        MASTER          = "yarn-client"
        H2O_PYTHON_WHEEL="${env.WORKSPACE}/private/h2o.whl"
        H2O_EXTENDED_JAR="${env.WORKSPACE}/assembly-h2o/private/"
        SPARK="spark-${params.sparkVersion}-bin-hadoop${HADOOP_VERSION}"
    }

    stages {

        stage('Git Checkout and Preparation'){
            steps{
                //checkout scm
                git url: 'https://github.com/h2oai/sparkling-water.git', branch: 'master'
                sh """
                if [ ! -d "${env.SPARK}" ]; then
                        wget -q "http://d3kbcqa49mib13.cloudfront.net/${env.SPARK}.tgz"
                        echo "Extracting spark JAR"
                        tar zxvf ${env.SPARK}.tgz
                fi

                echo 'Checkout and Preparation completed'
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

        stage('QA: Lint and Unit Tests') {

             steps {
                    sh """
                    # Build, run regular tests
                    ${env.WORKSPACE}/gradlew clean build
                    """

                    archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
		    }
			post {
				always {
                    junit 'core/build/test-results/test/*.xml'
					publishHTML target: [
						allowMissing: false,
					  	alwaysLinkToLastBuild: true,
					  	keepAll: true,
					  	reportDir: 'core/build/reports/tests/test',
					  	reportFiles: 'index.html',
					  	reportName: 'Core Unit tests'
					]
					publishHTML target: [
						allowMissing: false,
					  	alwaysLinkToLastBuild: true,
					  	keepAll: true,
					  	reportDir: 'examples/build/reports/tests/test',
					  	reportFiles: 'index.html',
					  	reportName: 'Examples Unit tests'
					]

				}
			}
        }

        stage('QA:Local Integration Tests') {

             steps {
                    sh """
                    # Build, run regular tests
                    ${env.WORKSPACE}/gradlew integTest -PsparkHome=${env.SPARK_HOME}
                    """

                    archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
		    }
			post {
				always {
                    junit 'examples/build/test-results/integTest/*.xml'
					publishHTML target: [
						allowMissing: false,
					  	alwaysLinkToLastBuild: true,
					  	keepAll: true,
					  	reportDir: 'core/build/reports/tests/integTest',
					  	reportFiles: 'index.html',
					  	reportName: 'Core: Integration tests'
					]
					publishHTML target: [
						allowMissing: false,
					  	alwaysLinkToLastBuild: true,
					  	keepAll: true,
					  	reportDir: 'examples/build/reports/tests/integTest',
					  	reportFiles: 'index.html',
					  	reportName: 'Examples Integration tests'
					]

				}
			}
        }

        stage('QA: Script Tests') {

             steps {
                    sh """
                    # Build, run regular tests
                    ${env.WORKSPACE}/gradlew scriptTest
                    """

                    archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
		    }
			post {
				always {
                    junit 'examples/build/test-results/scriptsTest/*.xml'
					publishHTML target: [
						allowMissing: false,
					  	alwaysLinkToLastBuild: true,
					  	keepAll: true,
					  	reportDir: 'examples/build/reports/tests/scriptsTest',
					  	reportFiles: 'index.html',
					  	reportName: 'Examples Script Tests'
					]
				}
			}
        }

        stage('QA: Distributed Integration tests') {

            steps {

                parallel(
                        sparklingintegyarn:{

                        sh "${env.WORKSPACE}/gradlew integTest -PbackendMode=${params.backendMode} -PstartH2OClusterOnYarn -PsparklingTestEnv=$sparklingTestEnv -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest"

                        },

                        sparklinginteg:{

                        sh "${env.WORKSPACE}/gradlew integTest -PbackendMode=${params.backendMode} -PsparklingTestEnv=$sparklingTestEnv -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest"

                        },

                        pysparklingyarn:{
                        sh """
                            ${env.WORKSPACE}/gradlew integTestPython -PbackendMode=${params.backendMode} -PstartH2OClusterOnYarn -PsparklingTestEnv=$sparklingTestEnv -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check
                            mkdir -p py/build/test-result
                            touch py/build/test-result/empty.xml
                        """
                        },
                        pysparkling:{
                        sh """
                            ${env.WORKSPACE}/gradlew integTestPython -PbackendMode=${params.backendMode} -PsparklingTestEnv=$sparklingTestEnv -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check
                            mkdir -p py/build/test-result
                            touch py/build/test-result/empty.xml
                        """
                        },
                        failFast: false

                )

            }
			post {
				always {
				    archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                    junit 'examples/build/test-results/integrationTest/*.xml'
					publishHTML target: [
						allowMissing: false,
					  	alwaysLinkToLastBuild: true,
					  	keepAll: true,
					  	reportDir: 'examples/build/reports/tests/distributedIntegTest',
					  	reportFiles: 'index.html',
					  	reportName: 'Examples Integration Tests'
					]
					archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
				}
			}

        }

        stage('Publish artifacts to S3') {

            when {
                expression { params.nightlyBuild == true }
            }
            steps{
                s3publish_artifacts(params.artifactDirectory, params.branchName, params.buildNumber)
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