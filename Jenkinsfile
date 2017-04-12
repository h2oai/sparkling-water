#!/usr/bin/groovy

pipeline{

    agent { label 'mr-0xd3' }

    parameters {
        string(name: 'branchName', defaultValue: 'master', description: 'Test given branch on top of YARN.')
        string(name: 'hdpVersion', defaultValue: 'current', description: 'HDP version to pass to Spark configuration - for example, 2.2.0.0-2041, or 2.6.0.2.2, or current. When running external tests on yarn, the current will not do since it is not automatically expanded -> so please set 2.2.6.3-1')

        booleanParam(name: 'runBuildTests', defaultValue: true, description: 'Run build tests - junit and local integration tests')
        booleanParam(name: 'runScriptTests', defaultValue: true, description: 'Run script tests')
        booleanParam(name: 'runIntegTests', defaultValue: true, description: 'Run integration tests')
        booleanParam(name: 'startH2OClusterOnYarn', defaultValue: false)
        booleanParam(name: 'runPySparklingIntegTests', defaultValue: false, description: 'Run pySparkling integration tests')

        choice(
            choices: '2.1.0\n2.0.2\n2.0.1\n2.0.0\n1.6.3\n1.6.2\n1.6.1\n1.6.0\n1.5.2\n1.4.2',
            description: 'Version of Spark used for testing.',
            name: 'sparkVersion')

        string(name: 'sparklingTestEnv', defaultValue: 'yarn', description: 'Sparkling water test profile (default yarn)')
        string(name: 'backendMode', defaultValue: 'internal', description: '')
        string(name: 'driverHadoopVersion', defaultValue: 'hdp2.2', description: 'Hadoop version for which h2o driver will be obtained')

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
                    ${env.WORKSPACE}/gradlew clean build -PbackendMode=${params.backendMode}
                    """

                    archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
		    }
			post {
				always {
                    junit: 'core/build/test-results/test/*.xml'
					publishHTML target: [
						allowMissing: false,
					  	alwaysLinkToLastBuild: true,
					  	keepAll: true,
					  	reportDir: 'core/build/reports/tests/test',
					  	reportFiles: 'index.html',
					  	reportName: 'Core: Unit tests'
					]
					publishHTML target: [
						allowMissing: false,
					  	alwaysLinkToLastBuild: true,
					  	keepAll: true,
					  	reportDir: 'examples/build/reports/tests/test',
					  	reportFiles: 'index.html',
					  	reportName: 'Examples: Unit tests'
					]

				}
			}
        }

        stage('QA:Local Integration Tests') {

             steps {
                    sh """
                    # Build, run regular tests
                    ${env.WORKSPACE}/gradlew integTest -PbackendMode=${params.backendMode} -PsparklingTestEnv=${params.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
                        """

                    archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
		    }
			post {
				always {
                    junit: 'examples/build/test-results/integTest/*.xml'
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
					  	reportName: 'Examples: Integration tests'
					]

				}
			}
        }

        stage('QA: Script Tests') {

             steps {
                    sh """
                    # Build, run regular tests
                    ${env.WORKSPACE}/gradlew scriptTest -PbackendMode=${params.backendMode}
                    """

                    archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
		    }
			post {
				always {
                    junit: 'examples/build/test-results/scriptsTest/*.xml'
					publishHTML target: [
						allowMissing: false,
					  	alwaysLinkToLastBuild: true,
					  	keepAll: true,
					  	reportDir: 'examples/build/reports/tests/scriptsTest',
					  	reportFiles: 'index.html',
					  	reportName: 'Examples: Script tests'
					]
				}
			}
        }


        stage('Stashing') {

            steps {
                // Make a tar of the directory and stash it -> --ignore-failed-read
                sh "tar  -zcvf ../stash_archive.tar.gz ."
                sh "mv ../stash_archive.tar.gz ."

                stash name: 'unit-test-stash', includes: 'stash_archive.tar.gz'
                echo 'Stash successful'

                sh "ls -ltrh ${env.WORKSPACE}"
                echo "Deleting the original workspace after stashing the directory"
                sh "rm -r ${env.WORKSPACE}/*"
                echo "Workspace Directory deleted"
            }
        }


        stage('QA:Integration tests') {

            steps {
                echo "Unstash the unit test"
                unstash "unit-test-stash"

                //Untar the archieve
                sh "tar -zxvf stash_archive.tar.gz"
                sh "ls -ltrh ${env.WORKSPACE}"
                
                sh """
                    # Move the unstashed directory outside the stashed one for the environment variables to pick up properly
                    #mv ${env.WORKSPACE}/unit-test-stash/* ${env.WORKSPACE}
                    #rm -r ${env.WORKSPACE}/unit-test-stash
                    """

                sh """
                     if [ "${params.runIntegTests}" = true -a "${params.startH2OClusterOnYarn}" = true ]; then
                             ${env.WORKSPACE}/gradlew integTest -PbackendMode=${params.backendMode} -PstartH2OClusterOnYarn -PsparklingTestEnv=$sparklingTestEnv -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
                     fi
                     if [ "${params.runIntegTests}" = true -a "${params.startH2OClusterOnYarn}" = false ]; then
                            ${env.WORKSPACE}/gradlew integTest -PbackendMode=${params.backendMode} -PsparklingTestEnv=${params.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
                     fi
                     #  echo 'Archiving artifacts after Integration test'

                 """

                 archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
            }
        }




  /*      stage('QA:Integration test- pySparkling'){

            steps{
                sh"""

                     # Run pySparkling integration tests on top of YARN
                     #
                     if [ "params.runPySparklingIntegTests" = true -a "param.startH2OClusterOnYarn" = true ]; then
                             ${env.WORKSPACE}/gradlew integTestPython -PbackendMode=${params.backendMode} -PstartH2OClusterOnYarn -PsparklingTestEnv=${params.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check
                             # manually create empty test-result/empty.xml file so Publish JUnit test result report does not fail when only pySparkling integration tests parameter has been selected
                             mkdir -p py/build/test-result
                             touch py/build/test-result/empty.xml
                     fi
                     if [ "params.runPySparklingIntegTests" = true -a "params.startH2OClusterOnYarn" = false ]; then
                             ${env.WORKSPACE}/gradlew integTestPython -PbackendMode=${params.backendMode} -PsparklingTestEnv=${params.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check
                             # manually create empty test-result/empty.xml file so Publish JUnit test result report does not fail when only pySparkling integration tests parameter has been selected
                             mkdir -p py/build/test-result
                             touch py/build/test-result/empty.xml
                     fi

                    #echo 'Archiving artifacts after Integration test- pySparkling'
                    echo 'Finished integration test'

                 """

             */   //archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
    //    //    }
       // }

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



