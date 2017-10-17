#!/usr/bin/groovy

def call(params, body) {
    def config = [:]
    body.resolveStrategy = Closure.DELEGATE_FIRST
    body.delegate = config
    body(params)

    def customEnv = [
            "SPARK=spark-${config.sparkVersion}-bin-hadoop${config.hadoopVersion}",
            "SPARK_HOME=${env.WORKSPACE}/spark",
            "HADOOP_CONF_DIR=/etc/hadoop/conf",
            "MASTER=yarn-client",
            "H2O_PYTHON_WHEEL=${env.WORKSPACE}/private/h2o.whl",
            "H2O_EXTENDED_JAR=${env.WORKSPACE}/assembly-h2o/private/extended/h2odriver-extended.jar",
            "JAVA_HOME=${JAVA_HOME_8}",
            "PATH=${JAVA_HOME}/bin:${PATH}",

            // Properties used in case we are building against specific H2O version
            "BUILD_HADOOP=true",
            "H2O_TARGET=${config.driverHadoopVersion}",
            "H2O_ORIGINAL_JAR=${env.WORKSPACE}/h2o-3/h2o-hadoop/h2o-${config.driverHadoopVersion}-assembly/build/libs/h2odriver.jar"
    ]

    node('dX-hadoop'){
        ansiColor('xterm') {
            timestamps {
                withEnv(customEnv) {
                    timeout(time: 120, unit: 'MINUTES') {
                        prepareSparkEnvironment()(config)
                        prepareSparklingWaterEnvironment()(config)
                        buildAndLint()(config)
                        unitTests()(config)

                    }
                }
            }
        }
    }
}

def prepareSparkEnvironment() {
    return { config ->
        stage('Prepare Spark Environment') {
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

            sh """
        # Setup 
        echo "spark.driver.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
        echo "spark.yarn.am.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
        echo "spark.executor.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf

        echo "-Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/java-opts
       """
        }
    }
}


def prepareSparklingWaterEnvironment(){
    return { config ->
        stage('QA: Prepare Sparkling Water Environment') {

            // Clean previous H2O Jars
            dir("${env.WORKSPACE}/assembly-h2o/private") {
                deleteDir()
            }

            sh """
            # Check if we are bulding against specific H2O branch
            if [ ${config.buildAgainstH2OBranch} = true ]; then
                # Clone H2O
                rm -rf h2o-3
                git clone https://github.com/h2oai/h2o-3.git
                cd h2o-3
                git checkout ${config.h2oBranch}
                ./gradlew build -x check
                cd ..
                if [ ${config.backendMode} = external ]; then
                    # In this case, PySparkling build is driven by H2O_HOME property
                    # When extending from specific jar the jar has already the desired name
                    ./gradlew -q extendJar
                fi
            fi
    
            # Check if we are building against included H2O 
            if [ ${config.buildAgainstH2OBranch} = false ]; then
                # Download h2o-python client, save it in private directory
                # and export variable H2O_PYTHON_WHEEL driving building of pysparkling package
                mkdir -p ${env.WORKSPACE}/private/
                curl -s `./gradlew -q printH2OWheelPackage` > ${env.WORKSPACE}/private/h2o.whl
                if [ ${config.backendMode} = external ]; then
                    cp `./gradlew -q extendJar -PdownloadH2O=${config.driverHadoopVersion}` ${env.H2O_EXTENDED_JAR}
                fi
            fi
    
        """
        }
    }
}

def buildAndLint(){
    return { config ->
        stage('QA: Build and Lint') {
            sh """
            # Build
            if [ ${config.buildAgainstH2OBranch} = true ]; then
                H2O_HOME=${env.WORKSPACE}/h2o-3 ${env.WORKSPACE}/gradlew clean --include-build ${env.WORKSPACE}/h2o-3 build -x check scalaStyle
            else
                ${env.WORKSPACE}/gradlew clean build -x check scalaStyle
            fi
            """
        }
    }
}

def unitTests() {
    return { config ->
        stage('QA: Unit Tests') {
            if (env.runUnitTests) {
                steps {
                    sh """
                    # Run unit tests
                    ${env.WORKSPACE}/gradlew test -x integTest -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto
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
        }
    }

}

return this