#!/usr/bin/groovy

def getNextNightlyBuildNumber(config) {
    if (config.buildAgainstH2OBranch.toBoolean()){
        return new Date().format('Y_m_d_H_M_S').toString()
    } else {
        try {
            def buildNumber = "https://h2o-release.s3.amazonaws.com/sparkling-water/${BRANCH_NAME}/nightly/latest".toURL().getText().toInteger()
            return buildNumber + 1
        } catch(Exception ignored){
            return 1
        }
    }
}

String getVersion(config) {
    def versionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('version') }
    def version = versionLine.split("=")[1]
    if (config.uploadNightly.toBoolean() && !config.buildAgainstH2OBranch.toBoolean()) {
        return "${version}-${getNextNightlyBuildNumber(config)}"
    } else {
        return version
    }
}

def getGradleCommand(config) {
    def cmd = "${env.WORKSPACE}/gradlew -Pversion=${getVersion(config)} -PtestMojoPipeline=true -Dorg.gradle.internal.launcher.welcomeMessageEnabled=false"

    if (config.buildAgainstH2OBranch.toBoolean()) {
        cmd = "H2O_HOME=${env.WORKSPACE}/h2o-3 ${cmd} --include-build ${env.WORKSPACE}/h2o-3"
    } else if(config.buildAgainstNightlyH2O.toBoolean()) {
        def h2oNightlyBuildVersion = new URL("http://h2o-release.s3.amazonaws.com/h2o/master/latest").getText().trim()
        def h2oNightlyMajorVersion = new URL("http://h2o-release.s3.amazonaws.com/h2o/master/${h2oNightlyBuildVersion}/project_version").getText().trim()
        h2oNightlyMajorVersion = h2oNightlyMajorVersion.substring(0, h2oNightlyMajorVersion.lastIndexOf('.'))
        cmd = "${cmd} -Ph2oMajorName=master -Ph2oMajorVersion=${h2oNightlyMajorVersion} -Ph2oBuild=${h2oNightlyBuildVersion}"
    }

    return cmd
}

def call(params, body) {
    def config = [:]
    body.resolveStrategy = Closure.DELEGATE_FIRST

    body.delegate = config
    body(params)
    config.put("gradleCmd", getGradleCommand(config))

    def customEnv = [
            "SPARK=spark-${config.sparkVersion}-bin-hadoop${config.hadoopVersion}",
            "SPARK_HOME=${env.WORKSPACE}/spark",
            "HADOOP_CONF_DIR=/etc/hadoop/conf",
            "MASTER=yarn-client",
            "H2O_EXTENDED_JAR=${env.WORKSPACE}/assembly-h2o/private/extended/h2odriver-extended.jar",
            // Properties used in case we are building against specific H2O version
            "BUILD_HADOOP=true",
            "H2O_TARGET=${config.driverHadoopVersion}",
            "H2O_ORIGINAL_JAR=${env.WORKSPACE}/h2o-3/h2o-hadoop-2/h2o-${config.driverHadoopVersion}-assembly/build/libs/h2odriver.jar"
    ]

    ansiColor('xterm') {
        timestamps {
            withEnv(customEnv) {
                timeout(time: 180, unit: 'MINUTES') {
                    dir("${env.WORKSPACE}") {
                        prepareSparkEnvironment()(config)
                        prepareSparklingWaterEnvironment()(config)
                        buildAndLint()(config)
                        unitTests()(config)
                        pyUnitTests()(config)
                        rUnitTests()(config)
                        localIntegTest()(config)
                        localPyIntegTest()(config)
                        scriptsTest()(config)
                        // Run Integration tests on YARN
                        node("dX-hadoop") {
                            def customEnvNew = [
                                    "SPARK=spark-${config.sparkVersion}-bin-hadoop${config.hadoopVersion}",
                                    "SPARK_HOME=${env.WORKSPACE}/spark",
                                    "HADOOP_CONF_DIR=/etc/hadoop/conf",
                                    "MASTER=yarn-client",
                                    "H2O_EXTENDED_JAR=${env.WORKSPACE}/assembly-h2o/private/extended/h2odriver-extended.jar",
                                    "JAVA_HOME=/usr/lib/jvm/java-8-oracle/",
                                    "PATH=/usr/lib/jvm/java-8-oracle/bin:${PATH}",
                                    // Properties used in case we are building against specific H2O version
                                    "BUILD_HADOOP=true",
                                    "H2O_TARGET=${config.driverHadoopVersion}",
                                    "H2O_ORIGINAL_JAR=${env.WORKSPACE}/h2o-3/h2o-hadoop-2/h2o-${config.driverHadoopVersion}-assembly/build/libs/h2odriver.jar"
                            ]
                            withEnv(customEnvNew) {
                                integTest()(config)
                            }
                        }
                        pysparklingIntegTest()(config)
                        publishNightly()(config)
                    }
                }
            }
        }
    }
}

String getDockerImageVersion() {
    def versionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('dockerImageVersion') }
    return versionLine.split("=")[1]
}

def withDocker(config, code) {
    def image = 'opsh2oai/sparkling_water_tests:' + getDockerImageVersion()
    retryWithDelay(3, 120,{
        withCredentials([usernamePassword(credentialsId: "harbor.h2o.ai", usernameVariable: 'REGISTRY_USERNAME', passwordVariable: 'REGISTRY_PASSWORD')]) {
            sh "docker login -u $REGISTRY_USERNAME -p $REGISTRY_PASSWORD harbor.h2o.ai"
            sh "docker pull harbor.h2o.ai/${image}"
        }
    })
    docker.image(image).inside("--init --privileged --dns 172.16.0.200 -v /home/0xdiag:/home/0xdiag") {
        sh "activate_java_8"
        code()
    }
}



def prepareSparkEnvironment() {
    return { config ->
        stage('Prepare Spark Environment - ' + config.backendMode) {
            withDocker(config) {

                sh  """
                cp -R \${SPARK_HOME_2_1} ${env.SPARK_HOME}
                
                echo "spark.driver.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                echo "spark.yarn.am.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                echo "spark.executor.extraJavaOptions -Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/spark-defaults.conf
                echo "-Dhdp.version="${config.hdpVersion}"" >> ${env.SPARK_HOME}/conf/java-opts
                """
            }
        }
    }
}

def prepareSparklingWaterEnvironment() {
    return { config ->
        stage('QA: Prepare Sparkling Water Environment - ' + config.backendMode) {
            withDocker(config) {
                sh """
                # Check if we are bulding against specific H2O branch
                if [ ${config.buildAgainstH2OBranch} = true ]; then
                    # Clone H2O
                    git clone https://github.com/h2oai/h2o-3.git
                    cd h2o-3
                    git checkout ${config.h2oBranch}
                    . /envs/h2o_env_python2.7/bin/activate
                    ./gradlew build -x check -x :h2o-r:build
                    cd ..
                    if [ ${config.backendMode} = external ]; then
                        # In this case, PySparkling build is driven by H2O_HOME property
                        # When extending from specific jar the jar has already the desired name
                        ${config.gradleCmd} -q :sparkling-water-examples:build -x check -PdoExtend extendJar
                    fi
                else if [ ${config.backendMode} = external ]; then
                        cp `${config.gradleCmd} -q :sparkling-water-examples:build -x check -PdoExtend extendJar -PdownloadH2O=${config.driverHadoopVersion}` ${env.H2O_EXTENDED_JAR}
                     fi
                fi
    
                """
            }
        }
    }
}


def buildAndLint() {
    return { config ->
        stage('QA: Build and Lint - ' + config.backendMode) {
            withDocker(config) {
                try {
                    withCredentials([usernamePassword(credentialsId: "LOCAL_NEXUS", usernameVariable: 'LOCAL_NEXUS_USERNAME', passwordVariable: 'LOCAL_NEXUS_PASSWORD')]) {
                        sh """
                            ${config.gradleCmd} clean build -x check scalaStyle -PlocalNexusUsername=$LOCAL_NEXUS_USERNAME -PlocalNexusPassword=$LOCAL_NEXUS_PASSWORD
                           """
                        stash 'sw-build'
                    }
                } finally {
                    arch 'assembly/build/reports/dependency-license/**/*'
                }
            }
        }
    }
}

def unitTests() {
    return { config ->
        stage('QA: Unit Tests - ' + config.backendMode) {
            withDocker(config) {
                if (config.runUnitTests.toBoolean()) {
                    try {
                        withCredentials([string(credentialsId: "DRIVERLESS_AI_LICENSE_KEY", variable: "DRIVERLESS_AI_LICENSE_KEY")]) {
                            if(config.backendMode == "external"){
                                sh "sudo -E /usr/sbin/startup.sh"
                            }
                            sh """
                            ${config.gradleCmd} test -x :sparkling-water-r:test -x :sparkling-water-py:test -x integTest -PbackendMode=${config.backendMode}
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
}

def pyUnitTests() {
    return { config ->

        stage('QA: Python Unit Tests 3.6 - ' + config.backendMode) {
            withDocker(config) {
                if (config.runPyUnitTests.toBoolean()) {
                    try {
                        withCredentials([string(credentialsId: "DRIVERLESS_AI_LICENSE_KEY", variable: "DRIVERLESS_AI_LICENSE_KEY")]) {
                            if(config.backendMode == "external"){
                                sh "sudo -E /usr/sbin/startup.sh"
                            }
                            sh """
                            ${config.gradleCmd} :sparkling-water-py:test -PpythonPath=/envs/h2o_env_python3.6/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -x integTest -PbackendMode=${config.backendMode}
                            """
                        }
                    } finally {
                        arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr, **/build/**/*log*, py/build/py_*_report.txt, **/build/reports/'
                    }

                }
            }
        }

        stage('QA: Python Unit Tests 2.7 - ' + config.backendMode) {
            withDocker(config) {
                if (config.runPyUnitTests.toBoolean()) {
                    try {
                        withCredentials([string(credentialsId: "DRIVERLESS_AI_LICENSE_KEY", variable: "DRIVERLESS_AI_LICENSE_KEY")]) {
                            if(config.backendMode == "external"){
                                sh "sudo -E /usr/sbin/startup.sh"
                            }
                            sh """
                            ${config.gradleCmd} :sparkling-water-py:test -PpythonPath=/envs/h2o_env_python2.7/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -x integTest -PbackendMode=${config.backendMode}
                            """
                        }
                    } finally {
                        arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr, **/build/**/*log*, py/build/py_*_report.txt, **/build/reports/'
                    }

                }
            }
        }

    }
}

def rUnitTests() {
    return { config ->
        stage('QA: RUnit Tests - ' + config.backendMode) {
            withDocker(config) {
                if (config.runRUnitTests.toBoolean()) {
                    try {
                        sh "sudo -E /usr/sbin/startup.sh"
                        sh """
                             ${config.gradleCmd} :sparkling-water-r:installH2ORPackage :sparkling-water-r:installRSparklingPackage
                             ${config.gradleCmd} :sparkling-water-r:test -x check -PbackendMode=${config.backendMode}
                             """
                    } finally {
                        arch '**/build/*tests.log,**/*.log, **/out.*, **/stdout, **/stderr, **/build/**/*log*, **/build/reports/'
                    }

                }
            }
        }

    }
}

def localIntegTest() {
    return { config ->
        stage('QA: Local Integration Tests - ' + config.backendMode) {
            withDocker(config) {
                if (config.runLocalIntegTests.toBoolean()) {
                    try {
                        if(config.backendMode == "external"){
                            sh "sudo -E /usr/sbin/startup.sh"
                        }
                        sh """
                        ${config.gradleCmd} integTest -x :sparkling-water-py:integTest -PsparkHome=${env.SPARK_HOME} -PbackendMode=${config.backendMode}
                        """
                    } finally {
                        arch '**/build/*tests.log, **/*.log, **/out.*, **/*py.out.txt, examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                        junit 'core/build/test-results/integTest/*.xml'
                        testReport 'core/build/reports/tests/integTest', 'Local Core Integration tests'
                        junit 'examples/build/test-results/integTest/*.xml'
                        testReport 'examples/build/reports/tests/integTest', 'Local Integration tests'
                    }
                }
            }
        }
    }
}

def localPyIntegTest() {
    return { config ->

        stage('QA: Local Py Integration Tests 3.6 - ' + config.backendMode) {
            withDocker(config) {
                if (config.runLocalPyIntegTests.toBoolean()) {
                    try {
                        sh "sudo -E /usr/sbin/startup.sh"
                        sh """
                        ${config.gradleCmd} sparkling-water-py:localIntegTestsPython -PpythonPath=/envs/h2o_env_python3.6/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -PsparkHome=${env.SPARK_HOME} -PbackendMode=${config.backendMode}
                        """
                    } finally {
                        arch '**/build/*tests.log, **/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                    }
                }
            }
        }
    }
}



def scriptsTest() {
    return { config ->
        stage('QA: Script Tests - ' + config.backendMode) {
            withDocker(config) {
                if (config.runScriptTests.toBoolean()) {
                    try {
                        if(config.backendMode == "external"){
                            sh "sudo -E /usr/sbin/startup.sh"
                        }
                        sh """
                        ${config.gradleCmd} scriptTest -PbackendMode=${config.backendMode}
                        """
                    } finally {
                        arch '**/build/*tests.log,**/*.log, **/out.*, **/stdout, **/stderr,**/build/**/*log*, **/build/reports/'
                        junit 'examples/build/test-results/scriptsTest/*.xml'
                        testReport 'examples/build/reports/tests/scriptsTest', 'Script Tests'
                    }
                }
            }
        }
    }
}


def integTest() {
    return { config ->
        stage('QA: Integration Tests - ' + config.backendMode) {
            cleanWs()
            unstash 'sw-build'
            if (config.runIntegTests.toBoolean()) {
                try {
                    sh """
                    ${config.gradleCmd} integTest -PbackendMode=${config.backendMode} -PsparklingTestEnv=${config.sparklingTestEnv} -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
                    """
                } finally {
                    arch '**/build/*tests.log, **/*.log, **/out.*, **/*py.out.txt, examples/build/test-results/binary/integTest/*, **/stdout, **/stderr,**/build/**/*log*,**/build/reports/'
                    junit 'examples/build/test-results/integTest/*.xml'
                    testReport 'examples/build/reports/tests/integTest', "Integration tests"
                }
            }
        }
    }
}

def pysparklingIntegTest() {
    return { config ->
        stage('QA: PySparkling Integration Tests 3.6 HDP 2.2 - ' + config.backendMode) {
            withDocker(config) {
                if (config.runPySparklingIntegTests.toBoolean()) {
                    try {
                        sh """
                         sudo -E /usr/sbin/startup.sh
                         ${config.gradleCmd} sparkling-water-py:yarnIntegTestsPython -PpythonPath=/envs/h2o_env_python3.6/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -PbackendMode=${config.backendMode} -PsparkHome=${env.SPARK_HOME}
                        """
                    } finally {
                        arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt, **/build/reports/'
                    }
                }
            }
        }
    }
}

def static getUploadPath(config) {
    if (config.buildAgainstH2OBranch.toBoolean()) {
        config.h2oBranch.replace("/", "_")
    } else {
        "nightly"
    }
}

def publishNightly() {
    return { config ->
        stage('Nightly: Publishing Artifacts to S3 - ' + config.backendMode) {
            withDocker(config) {
                if (config.uploadNightly.toBoolean()) {
                    withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'AWS S3 Credentials', accessKeyVariable: 'AWS_ACCESS_KEY_ID', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'],
                                     usernamePassword(credentialsId: "SIGNING_KEY", usernameVariable: 'SIGN_KEY', passwordVariable: 'SIGN_PASSWORD'),
                                     file(credentialsId: 'release-secret-key-ring-file', variable: 'RING_FILE_PATH')]) {

                        sh  """
                            ${config.gradleCmd} dist -PdoRelease -Psigning.keyId=${SIGN_KEY} -Psigning.secretKeyRingFile=${RING_FILE_PATH} -Psigning.password=

                            NEW_BUILD_VERSION=${getNextNightlyBuildNumber(config)}
                                                
                            export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
                            export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
                            ~/.local/bin/aws s3 sync dist/build/dist s3://h2o-release/sparkling-water/${BRANCH_NAME}/${getUploadPath(config)}/\${NEW_BUILD_VERSION}/ --acl public-read
                            
                            if [ ${config.buildAgainstH2OBranch} = false ]; then
                                echo UPDATE LATEST POINTER
                                echo \${NEW_BUILD_VERSION} > latest
                                echo "<head>" > latest.html
                                echo "<meta http-equiv=\\"refresh\\" content=\\"0; url=\${NEW_BUILD_VERSION}/index.html\\" />" >> latest.html
                                echo "</head>" >> latest.html
                                ~/.local/bin/aws s3 cp latest s3://h2o-release/sparkling-water/${BRANCH_NAME}/nightly/latest --acl public-read
                                ~/.local/bin/aws s3 cp latest.html s3://h2o-release/sparkling-water/${BRANCH_NAME}/nightly/latest.html --acl public-read
                                ~/.local/bin/aws s3 cp latest.html s3://h2o-release/sparkling-water/${BRANCH_NAME}/nightly/index.html --acl public-read
                            fi                    
                            """
                    }
                }
            }
        }
    }
}

return this
