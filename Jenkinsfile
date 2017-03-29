#!/usr/bin/groovy

pipeline{

    agent { label 'mr-0xd3' }

    parameters {
        string(name: 'branchName', defaultValue: 'master', description: 'Test given branch on top of YARN.')
        string(name: 'hdpVersion', defaultValue: 'current', description: 'HDP version to pass to Spark configuration - for example, 2.2.0.0-2041, or 2.6.0.2.2, or current. When running external tests on yarn, the current won't do since it's not automatically expanded -> so please set 2.2.6.3-1')

        booleanParam(name: 'runBuildTests', defaultValue: 'true', description: 'Run build tests - junit and local integration tests')
        booleanParam(name: 'runScriptTests', defaultValue: 'true', description: 'Run script tests')
        booleanParam(name: 'runIntegTests', defaultValue: 'true', description: 'Run integration tests')
        booleanParam(name: 'startH2OClusterOnYarn', defaultValue: 'false')
        booleanParam(name: 'runPySparklingIntegTests', defaultValue: 'false', description: 'Run pySparkling integration tests')

        choice(
            choices: '2.1.0\n2.0.2\n2.0.1\n2.0.0\n1.6.3\n1.6.2\n1.6.1\n1.6.0\n1.5.2\n1.4.2',
            description: 'Version of Spark used for testing.',
            name: 'sparkVersion')
        choice(
            choices: 'yarn',
            description: 'Sparkling water test profile (default yarn)',
            name: 'sparklingTestEnv')
        choice(
            choices: 'internal\nexternal',
            description: '',
            name: 'backendMode')
        choice(
            choices: 'hdp2.2\ncdh5.4',
            description: 'Hadoop version for which h2o driver will be obtained',
            name: 'driverHadoopVersion')
    }

    environment{
        SPARK_HOME="${env.WORKSPACE}/spark-2.1.0-bin-hadoop2.6"
        HADOOP_CONF_DIR="/etc/hadoop/conf"
        MASTER="yarn-client"
        R_LIBS_USER="${env.WORKSPACE}/Rlibrary"
        HDP_VERSION="${hdpVersion}"
        driverHadoopVersion="${driverHadoopVersion}"
        startH2OClusterOnYarn="${startH2OClusterOnYarn}"
        H2O_PYTHON_WHEEL="${env.WORKSPACE}/private/h2o.whl"
        H2O_EXTENDED_JAR="${env.WORKSPACE}/assembly-h2o/private/"
    }

    stages{

        /*  stage('init'){
                  def SPARK="spark-${sparkVersion}-bin-hadoop2.6"
          }
         */
        stage('Git Checkout and Preparation'){
            steps{

                //checkout scm
                git url: 'https://github.com/h2oai/sparkling-water.git',
                        branch: 'master'
                sh"""
                #git url: 'https://github.com/h2oai/sparkling-water.git'
                #def SPARK="spark-${sparkVersion}-bin-hadoop2.6"

                if [ ! -d "spark-2.1.0-bin-hadoop2.6" ]; then
                        wget "http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.6.tgz"
                        echo "Extracting spark JAR"
                        tar zxvf spark-2.1.0-bin-hadoop2.6.tgz
                fi

                echo 'Checkout and Preparation completed'
                sh"""
            }

        }

        stage('QA: build & lint'){

            steps{
                sh"""
                    mkdir -p ${env.WORKSPACE}/Rlibrary
                    echo "spark.driver.extraJavaOptions -Dhdp.version="${env.HDP_VERSION}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                    echo "spark.yarn.am.extraJavaOptions -Dhdp.version="${env.HDP_VERSION}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                    echo "spark.executor.extraJavaOptions -Dhdp.version="${env.HDP_VERSION}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf

                    echo "-Dhdp.version="${env.HDP_VERSION}"" >> ${env.SPARK_HOME}/conf/java-opts

                    mkdir -p ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-07.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-07.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-08.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-08.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-09.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-09.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-10.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-10.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-11.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-11.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/2013-12.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/2013-12.csv
                    cp /home/0xdiag/bigdata/laptop/citibike-nyc/31081_New_York_City__Hourly_2013.csv ${env.WORKSPACE}/examples/bigdata/laptop/citibike-nyc/31081_New_York_City__Hourly_2013.csv

                    # Download h2o-python client, save it in private directory
                    # and export variable H2O_PYTHON_WHEEL driving building of pysparkling package
                    mkdir -p ${env.WORKSPACE}/private/
                    curl `./gradlew -q printH2OWheelPackage ` > ${env.WORKSPACE}/private/h2o.whl
                    ./gradlew -q extendJar -PdownloadH2O=${env.driverHadoopVersion}


                 """
                //archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'

            }
        }


        stage('QA:Unit tests'){

            steps{
                sh """
                    # Build, run regular tests
                    if [ "$runBuildTests" = true ]; then
                            echo 'runBuildTests = True'
                           ${env.WORKSPACE}/gradlew clean build -PbackendMode=${backendMode}
                    else
                            echo 'runBuildTests = False'
                            ${env.WORKSPACE}/gradlew clean build -x check -PbackendMode=${backendMode}

                    fi

                    if [ "$runScriptTests" = true ]; then
                            echo 'runScriptTests = true'
                            ${env.WORKSPACE}/gradlew scriptTest -PbackendMode=${backendMode}

                    fi

                    # running integration just after unit test
                    ${env.WORKSPACE}/gradlew integTest -PbackendMode=${backendMode} -PsparklingTestEnv=$sparklingTestEnv -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest

                 """
               //archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
            }
        }

        stage('Stashing'){

            steps{
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


        stage('QA:Integration tests'){

            steps{
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
                     if [ "$runIntegTests" = true -a "$startH2OClusterOnYarn" = true ]; then
                             ${env.WORKSPACE}/gradlew integTest -PbackendMode=${backendMode} -PstartH2OClusterOnYarn -PsparklingTestEnv=$sparklingTestEnv -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
                     fi

                     if [ "$runIntegTests" = true -a "$startH2OClusterOnYarn" = false ]; then
                            ${env.WORKSPACE}/gradlew integTest -PbackendMode=${backendMode} -PsparklingTestEnv=$sparklingTestEnv -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
                     fi

                    """

                 //archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
            }
        }

        stage('QA:Integration test- pySparkling'){

            steps{
                sh"""

                     # Run pySparkling integration tests on top of YARN
                     #
                     if [ "$runPySparklingIntegTests" = true -a "$startH2OClusterOnYarn" = true ]; then
                             ${env.WORKSPACE}/gradlew integTestPython -PbackendMode=${backendMode} -PstartH2OClusterOnYarn -PsparklingTestEnv=$sparklingTestEnv -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check
                             # manually create empty test-result/empty.xml file so Publish JUnit test result report does not fail when only pySparkling integration tests parameter has been selected
                             mkdir -p py/build/test-result
                             touch py/build/test-result/empty.xml
                     fi
                     if [ "$runPySparklingIntegTests" = true -a "$startH2OClusterOnYarn" = false ]; then
                             ${env.WORKSPACE}/gradlew integTestPython -PbackendMode=${backendMode} -PsparklingTestEnv=$sparklingTestEnv -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check
                             # manually create empty test-result/empty.xml file so Publish JUnit test result report does not fail when only pySparkling integration tests parameter has been selected
                             mkdir -p py/build/test-result
                             touch py/build/test-result/empty.xml
                     fi

                    #echo 'Archiving artifacts after Integration test- pySparkling'
                    echo 'Finished integration test'

                 """

                //archiveArtifacts artifacts:'**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
            }
        }
    }
}
