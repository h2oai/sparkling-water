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
            "PATH=${JAVA_HOME_8}/bin:${PATH}",

            // Properties used in case we are building against specific H2O version
            "BUILD_HADOOP=true",
            "H2O_TARGET=${config.driverHadoopVersion}",
            "H2O_ORIGINAL_JAR=${env.WORKSPACE}/h2o-3/h2o-hadoop/h2o-${config.driverHadoopVersion}-assembly/build/libs/h2odriver.jar"
    ]

    ansiColor('xterm') {
        timestamps {
            withEnv(customEnv) {
                timeout(time: 120, unit: 'MINUTES') {
                    dir("${env.WORKSPACE}") {
                        prepareSparkEnvironment()(config)
                        prepareSparklingWaterEnvironment()(config)
                        buildAndLint()(config)
                        unitTests()(config)
                        localIntegTest()(config)
                        scriptsTest()(config)
                        integTest()(config)
                        pysparklingIntegTest()(config)
                    }
                }
            }
        }
    }
}

def prepareSparkEnvironment() {
    return { config ->
        stage('Prepare Spark Environment') {
            sh  """
                # Download Spark
                wget -q "http://d3kbcqa49mib13.cloudfront.net/${env.SPARK}.tgz"
                mkdir -p "${env.SPARK_HOME}"
                tar zxvf ${env.SPARK}.tgz -C "${env.SPARK_HOME}" --strip-components 1
                rm -rf ${env.SPARK}.tgz
                """

            sh  """
                # Setup Spark
                echo "spark.driver.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                echo "spark.yarn.am.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                echo "spark.executor.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                
                echo "-Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/java-opts
                """
        }
    }
}


def prepareSparklingWaterEnvironment() {
    return { config ->
        stage('QA: Prepare Sparkling Water Environment') {

            sh  """
                # Check if we are bulding against specific H2O branch
                if [ ${config.buildAgainstH2OBranch} = true ]; then
                    # Clone H2O
                    git clone https://github.com/h2oai/h2o-3.git
                    cd h2o-3
                    git checkout ${config.h2oBranch}
                    ${env.WORKSPACE}/gradlew build -x check
                    cd ..
                    if [ ${config.backendMode} = external ]; then
                        # In this case, PySparkling build is driven by H2O_HOME property
                        # When extending from specific jar the jar has already the desired name
                        ${env.WORKSPACE}/gradlew -q extendJar
                    fi
                else
                    # Download h2o-python client, save it in private directory
                    # and export variable H2O_PYTHON_WHEEL driving building of pysparkling package
                    mkdir -p ${env.WORKSPACE}/private/
                    curl -s `${env.WORKSPACE}/gradlew -q printH2OWheelPackage` > ${env.WORKSPACE}/private/h2o.whl
                    if [ ${config.backendMode} = external ]; then
                        cp `${env.WORKSPACE}/gradlew -q :sparkling-water-examples:build -x check extendJar -PdownloadH2O=${config.driverHadoopVersion}` ${env.H2O_EXTENDED_JAR}
                    fi
                fi
    
                """
        }
    }
}

def getGradleCommand(config) {
    if (config.buildAgainstH2OBranch.toBoolean()) {
        "H2O_HOME=${env.WORKSPACE}/h2o-3 ${env.WORKSPACE}/gradlew --include-build ${env.WORKSPACE}/h2o-3"
    } else {
        "${env.WORKSPACE}/gradlew"
    }
}

def buildAndLint() {
    return { config ->
        stage('QA: Build and Lint') {
            sh  """
                # Build
                ${getGradleCommand(config)} clean build -x check scalaStyle
                """
        }
    }
}

def unitTests() {
    return { config ->
        stage('QA: Unit Tests') {
            if (config.runUnitTests.toBoolean()) {
                sh """
                # Run unit tests
                ${getGradleCommand(config)} test -x integTest -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto
                """


                arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr, **/build/**/*log*, py/build/py_*_report.txt, **/build/reports/'
                junit 'core/build/test-results/test/*.xml'
                testReport 'core/build/reports/tests/test', 'Core Unit tests'
            }
        }

    }
}

def localIntegTest() {
    return { config ->
        stage('QA: Local Integration Tests') {

            if (config.runLocalIntegTests.toBoolean()) {
                sh  """
                    # Run local integration tests
                    ${getGradleCommand(config)} integTest -PsparkHome=${env.SPARK_HOME} -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto
                    """

                arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                junit 'examples/build/test-results/integTest/*.xml'
                testReport 'core/build/reports/tests/integTest', 'Local Core Integration tests'
                testReport 'examples/build/reports/tests/integTest', 'Local Examples Integration tests'
            }
        }
    }
}


def scriptsTest() {
    return { config ->
        stage('QA: Script Tests') {
            if (config.runScriptTests.toBoolean()) {
                sh  """
                    # Run scripts tests
                    ${getGradleCommand(config)} scriptTest -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto
                    """

                arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                junit 'examples/build/test-results/scriptsTest/*.xml'
                testReport 'examples/build/reports/tests/scriptsTest', 'Examples Script Tests'
            }
        }
    }
}


def integTest() {
    return { config ->
        stage('QA: Integration Tests') {
            if (config.runIntegTests.toBoolean()) {
                sh  """
                    ${getGradleCommand(config)} integTest -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto -PsparklingTestEnv=${config.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
                    #  echo 'Archiving artifacts after Integration test'
                    """

                arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                junit 'examples/build/test-results/integTest/*.xml'
                testReport 'examples/build/reports/tests/integTest', "${config.backendMode} Examples Integration tests"
            }
        }
    }
}

def pysparklingIntegTest() {
    return { config ->
        stage('QA: PySparkling Integration Tests') {
            if (config.runPySparklingIntegTests.toBoolean()) {

                sh  """
                     ${getGradleCommand(config)} integTestPython -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto -PsparklingTestEnv=${config.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check
                    #  echo 'Archiving artifacts after PySparkling Integration test'
                    """

                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt,examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
            }
        }
    }
}

return this