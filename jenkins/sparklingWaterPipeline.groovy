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
            "JAVA_HOME=/usr/lib/jvm/java-8-oracle/",
            "PATH=/usr/lib/jvm/java-8-oracle/bin:${PATH}",

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
                        publishNightly()(config)
                    }
                }
            }
        }
    }
}


def getGradleCommand(config) {
    def gradleStr
    if (config.buildAgainstH2OBranch.toBoolean()) {
        gradleStr = "H2O_HOME=${env.WORKSPACE}/h2o-3 ${env.WORKSPACE}/gradlew --include-build ${env.WORKSPACE}/h2o-3"
    } else {
        gradleStr = "${env.WORKSPACE}/gradlew"
    }

    if(config.buildAgainstSparkBranch.toBoolean()) {
        "${gradleStr} -x checkSparkVersionTask"
    }else{
        gradleStr
    }
}


def prepareSparkEnvironment() {
    return { config ->
        stage('Prepare Spark Environment - ' + config.backendMode) {
            
            if (config.buildAgainstSparkBranch.toBoolean()) {
                // build spark
                sh """
                    git clone https://github.com/apache/spark.git spark_repo
                    cd spark_repo
                    git checkout ${config.sparkBranch}
                    ./dev/make-distribution.sh --name custom-spark --pip -Phadoop-2.6 -Pyarn
                    cp -r ./dist/ ${env.SPARK_HOME}
                    """
            }else {
                sh  """
                    # Download Spark
                    wget -q "http://mirrors.ocf.berkeley.edu/apache/spark/spark-${config.sparkVersion}/${env.SPARK}.tgz"
                    mkdir -p "${env.SPARK_HOME}"
                    tar zxvf ${env.SPARK}.tgz -C "${env.SPARK_HOME}" --strip-components 1
                    rm -rf ${env.SPARK}.tgz
                    """
            }

            sh  """
                # Setup Spark
                echo "spark.driver.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                echo "spark.yarn.am.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                echo "spark.executor.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                
                echo "-Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/java-opts
                """
            if(config.buildAgainstSparkBranch.toBoolean()){
                sh  """
                    echo "spark.ext.h2o.spark.version.check.enabled false" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                    """
            }
        }
    }
}


def prepareSparklingWaterEnvironment() {
    return { config ->
        stage('QA: Prepare Sparkling Water Environment - ' + config.backendMode) {

            // Warm up Gradle wrapper. When the gradle wrapper is downloaded for the first time, it prints message
            // with release notes which can mess up the build
            sh  """
                ${env.WORKSPACE}/gradlew --help
                """
            
            // In case of nightly build, modify gradle.properties
            if(config.buildNightly.toBoolean()){

                def h2oNightlyBuildVersion = new URL("http://h2o-release.s3.amazonaws.com/h2o/master/latest").getText().trim()

                def h2oNightlyMajorVersion = new URL("http://h2o-release.s3.amazonaws.com/h2o/master/${h2oNightlyBuildVersion}/project_version").getText().trim()
                h2oNightlyMajorVersion = h2oNightlyMajorVersion.substring(0, h2oNightlyMajorVersion.lastIndexOf('.'))

                sh  """
                     sed -i.backup -E "s/h2oMajorName=.*/h2oMajorName=master/" gradle.properties
                     sed -i.backup -E "s/h2oMajorVersion=.*/h2oMajorVersion=${h2oNightlyMajorVersion}/" gradle.properties
                     sed -i.backup -E "s/h2oBuild=.*/h2oBuild=${h2oNightlyBuildVersion}/" gradle.properties
                     sed -i.backup -E "s/\\.[0-9]+-SNAPSHOT/.\${BUILD_NUMBER}_nightly/" gradle.properties
                    """
            }

            sh  """
                # Check if we are bulding against specific H2O branch
                if [ ${config.buildAgainstH2OBranch} = true ]; then
                    # Clone H2O
                    git clone https://github.com/h2oai/h2o-3.git
                    cd h2o-3
                    git checkout ${config.h2oBranch}
                    ./gradlew build -x check
                    cd ..
                    if [ ${config.backendMode} = external ]; then
                        # In this case, PySparkling build is driven by H2O_HOME property
                        # When extending from specific jar the jar has already the desired name
                        ${getGradleCommand(config)} -q :sparkling-water-examples:build -x check -PdoExtend extendJar
                    fi
                else
                    # Download h2o-python client, save it in private directory
                    # and export variable H2O_PYTHON_WHEEL driving building of pysparkling package
                    mkdir -p ${env.WORKSPACE}/private/
                    curl -s `${env.WORKSPACE}/gradlew -q printH2OWheelPackage` > ${env.WORKSPACE}/private/h2o.whl
                    if [ ${config.backendMode} = external ]; then
                        cp `${getGradleCommand(config)} -q :sparkling-water-examples:build -x check -PdoExtend extendJar -PdownloadH2O=${config.driverHadoopVersion}` ${env.H2O_EXTENDED_JAR}
                    fi
                fi
    
                """
        }
    }
}


def buildAndLint() {
    return { config ->
        stage('QA: Build and Lint - ' + config.backendMode) {
            withCredentials([usernamePassword(credentialsId: "LOCAL_NEXUS", usernameVariable: 'LOCAL_NEXUS_USERNAME', passwordVariable: 'LOCAL_NEXUS_PASSWORD')]) {
                sh  """
                    # Build
                    ${getGradleCommand(config)} clean build -x check scalaStyle -PlocalNexusUsername=$LOCAL_NEXUS_USERNAME -PlocalNexusPassword=$LOCAL_NEXUS_PASSWORD
                    """
            }
        }
    }
}

def unitTests() {
    return { config ->
        stage('QA: Unit Tests - ' + config.backendMode) {
            if (config.runUnitTests.toBoolean()) {
                try {
                    withCredentials([string(credentialsId: "DRIVERLESS_AI_LICENSE_KEY", variable: "DRIVERLESS_AI_LICENSE_KEY")]) {
                        sh  """
                            # Run unit tests
                            ${getGradleCommand(config)} test -x integTest -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto
                            """
                    }
                } finally {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr, **/build/**/*log*, py/build/py_*_report.txt, **/build/reports/'
                    junit 'core/build/test-results/test/*.xml'
                    junit 'ml/build/test-results/test/*.xml'
                    testReport 'core/build/reports/tests/test', 'Core Unit tests'
                    testReport 'ml/build/reports/tests/test', "ML Unit Tests"
                }

            }
        }

    }
}

def localIntegTest() {
    return { config ->
        stage('QA: Local Integration Tests - ' + config.backendMode) {

            if (config.runLocalIntegTests.toBoolean()) {
                try {
                    sh  """
                        # Run local integration tests
                        ${getGradleCommand(config)} integTest -PsparkHome=${env.SPARK_HOME} -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto
                        """
                } finally {
                    arch '**/build/*tests.log, **/*.log, **/out.*, **/*py.out.txt, examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                    junit 'examples/build/test-results/integTest/*.xml'
                    testReport 'core/build/reports/tests/integTest', 'Local Core Integration tests'
                    testReport 'examples/build/reports/tests/integTest', 'Local Integration tests'
                }
            }
        }
    }
}


def scriptsTest() {
    return { config ->
        stage('QA: Script Tests - ' + config.backendMode) {
            if (config.runScriptTests.toBoolean()) {
                try {
                    sh  """
                        # Run scripts tests
                        ${getGradleCommand(config)} scriptTest -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto
                        """
                } finally {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr,**/build/**/*log*, **/build/reports/'
                    junit 'examples/build/test-results/scriptsTest/*.xml'
                    testReport 'examples/build/reports/tests/scriptsTest', 'Script Tests'
                }
            }
        }
    }
}


def integTest() {
    return { config ->
        stage('QA: Integration Tests - ' + config.backendMode) {
            if (config.runIntegTests.toBoolean()) {
                try {
                    sh  """
                    ${getGradleCommand(config)} integTest -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto -PsparklingTestEnv=${config.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
                    #  echo 'Archiving artifacts after Integration test'
                    """
                } finally {
                    arch '**/build/*tests.log, **/*.log, **/out.*, **/*py.out.txt, examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                    junit 'examples/build/test-results/integTest/*.xml'
                    testReport 'examples/build/reports/tests/integTest', "Integration tests"
                }
            }
        }
    }
}

def pysparklingIntegTest() {
    return { config ->
        stage('QA: PySparkling Integration Tests - ' + config.backendMode) {
            if (config.runPySparklingIntegTests.toBoolean()) {
                try{
                    sh  """
                         ${getGradleCommand(config)} integTestPython -PbackendMode=${config.backendMode} -PexternalBackendStartMode=auto -PsparklingTestEnv=${config.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check
                         # echo 'Archiving artifacts after PySparkling Integration test'
                        """
                } finally {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt, **/build/reports/'

                }
            }
        }
    }
}

def publishNightly(){
    return { config ->
        stage ('Nightly: Publishing Artifacts to S3 - ' + config.backendMode){
            if (config.buildNightly.toBoolean() && config.uploadNightly.toBoolean()) {

                withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'AWS S3 Credentials', accessKeyVariable: 'AWS_ACCESS_KEY_ID', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {

                    sh """
                    # echo 'Making distribution'
                    ${getGradleCommand(config)} buildSparklingWaterDist

                    # Upload to S3
                    """

                    def tmpdir = "./buildsparklingwater.tmp"
                    sh """
                    # Publish the output to S3.
                    echo
                    echo PUBLISH
                    echo
                    s3cmd --rexclude='target/classes/*' --acl-public sync ${env.WORKSPACE}/dist/build/ s3://h2o-release/sparkling-water/${BRANCH_NAME}/${BUILD_NUMBER}_nightly/
                    
                    echo EXPLICITLY SET MIME TYPES AS NEEDED
                    list_of_html_files=`find dist/build -name '*.html' | sed 's/dist\\/build\\///g'`
                    echo \${list_of_html_files}
                    for f in \${list_of_html_files}
                    do
                        s3cmd --acl-public --mime-type text/html put dist/build/\${f} s3://h2o-release/sparkling-water/${BRANCH_NAME}/${BUILD_NUMBER}_nightly/\${f}
                    done
                    
                    list_of_js_files=`find dist/build -name '*.js' | sed 's/dist\\/build\\///g'`
                    echo \${list_of_js_files}
                    for f in \${list_of_js_files}
                    do
                        s3cmd --acl-public --mime-type text/javascript put dist/build/\${f} s3://h2o-release/sparkling-water/\${BRANCH_NAME}/\${BUILD_NUMBER}_nightly/\${f}
                    done
                    
                    list_of_css_files=`find dist/build -name '*.css' | sed 's/dist\\/build\\///g'`
                    echo \${list_of_css_files}
                    for f in \${list_of_css_files}
                    do
                        s3cmd --acl-public --mime-type text/css put dist/build/\${f} s3://h2o-release/sparkling-water/${BRANCH_NAME}/${BUILD_NUMBER}_nightly/\${f}
                    done
                    
                    echo UPDATE LATEST POINTER
                    mkdir -p ${tmpdir}
                    echo ${BUILD_NUMBER}_nightly > ${tmpdir}/latest
                    echo "<head>" > ${tmpdir}/latest.html
                    echo "<meta http-equiv=\\"refresh\\" content=\\"0; url=${BUILD_NUMBER}_nightly/index.html\\" />" >> ${tmpdir}/latest.html
                    echo "</head>" >> ${tmpdir}/latest.html
                    s3cmd --acl-public put ${tmpdir}/latest s3://h2o-release/sparkling-water/${BRANCH_NAME}/latest
                    s3cmd --acl-public put ${tmpdir}/latest.html s3://h2o-release/sparkling-water/${BRANCH_NAME}/latest.html
                    s3cmd --acl-public put ${tmpdir}/latest.html s3://h2o-release/sparkling-water/${BRANCH_NAME}/index.html
                                        
                    """
                }

                // Update the links
                sh  """
                    git clone git@github.com:h2oai/docs.h2o.ai.git
                    cd docs.h2o.ai/sites-available/
                    sed -i.backup -E "s?http://h2o-release.s3.amazonaws.com/sparkling-water/master/[0-9]+_nightly/?http://h2o-release.s3.amazonaws.com/sparkling-water/master/${BUILD_NUMBER}_nightly/?" 000-default.conf
                    git add 000-default.conf
                    git commit -m "Update links of Sparkling Water nighly version to ${BUILD_NUMBER}_nightly"
                    git push --set-upstream origin master
                    """
            }
        }
    }
}
return this