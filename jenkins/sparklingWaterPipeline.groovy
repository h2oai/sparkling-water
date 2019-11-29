#!/usr/bin/groovy

//
// Utility methods for the pipeline
//

def getS3Path(config) {
    return sh(script: "${config.gradleCmd} -q s3path", returnStdout: true).trim()
}

String getNightlyVersion(config) {
    def sparkMajorVersion = config.sparkMajorVersion
    def version = readFile("gradle.properties").split("\n").find() { line -> line.startsWith("version") }.split("=")[1]
    def versionParts = version.split("-")
    def h2oPart = versionParts[0]
    def swPatch = versionParts[1]
    def swNightlyBuildNumber
    try {
        def lastVersion = "https://h2o-release.s3.amazonaws.com/sparkling-water/spark-${config.sparkMajorVersion}/${getS3Path(config)}latest".toURL().getText().toString()
        def lastVersionParts = lastVersion.split("-")
        def lastH2OPart = lastVersionParts[0]
        def lastSWPart = lastVersionParts[1]
        if (lastSWPart.contains(".")) {
            def lastSWParts = lastSWPart.split("\\.")
            def lastSWPatch = lastSWParts[0]
            def lastSWBuild = lastSWParts[1]
            if (lastH2OPart != h2oPart || lastSWPatch != swPatch) {
                swNightlyBuildNumber = 1 // reset the nightly build number
            } else {
                swNightlyBuildNumber = lastSWBuild.toInteger() + 1
            }
        } else {
            swNightlyBuildNumber = 1
        }
    } catch (Exception ignored) {
        swNightlyBuildNumber = 1
    }
    return "${h2oPart}-${swPatch}.${swNightlyBuildNumber}-${sparkMajorVersion}"
}

String getSparkVersion(config) {
    def sparkMajorVersion = config.sparkMajorVersion
    def versionLine = readFile("gradle-spark${sparkMajorVersion}.properties").split("\n").find() { line -> line.startsWith('sparkVersion') }
    return versionLine.split("=")[1]
}

def getGradleCommand(config) {
    def cmd = "${env.WORKSPACE}/gradlew -PisNightlyBuild=${config.uploadNightly} -Pspark=${config.sparkMajorVersion} -PsparkVersion=${getSparkVersion(config)} -PtestMojoPipeline=true -Dorg.gradle.internal.launcher.welcomeMessageEnabled=false"
    if (config.buildAgainstH2OBranch.toBoolean()) {
        return "H2O_HOME=${env.WORKSPACE}/h2o-3 ${cmd} -PbuildAgainstH2OBranch=${config.h2oBranch} --include-build ${env.WORKSPACE}/h2o-3"
    } else {
        return cmd
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

def withSharedSetup(sparkMajorVersion, config, code) {
    node('docker && micro') {
        docker.withRegistry("http://harbor.h2o.ai") {
            ws("${env.WORKSPACE}-spark-${sparkMajorVersion}-${config.backendMode}") {
                config.put("sparkMajorVersion", sparkMajorVersion)

                cleanWs()
                checkout scm

                config.put("gradleCmd", getGradleCommand(config))
                config.put("sparkVersion", getSparkVersion(config))

                def customEnv = [
                        "SPARK=spark-${config.sparkVersion}-bin-hadoop${config.hadoopVersion}",
                        "SPARK_HOME=${env.WORKSPACE}/spark",
                        "HADOOP_CONF_DIR=/etc/hadoop/conf",
                        "MASTER=yarn-client",
                        "H2O_EXTENDED_JAR=${env.WORKSPACE}/assembly-h2o/private/extended/h2odriver-extended.jar",
                        "H2O_DRIVER_JAR=${env.WORKSPACE}/assembly-h2o/private/extended/h2odriver.jar",
                        // Properties used in case we are building against specific H2O version
                        "BUILD_HADOOP=true",
                        "H2O_TARGET=${config.driverHadoopVersion}",
                        "H2O_ORIGINAL_JAR=${env.WORKSPACE}/h2o-3/h2o-hadoop-2/h2o-${config.driverHadoopVersion}-assembly/build/libs/h2odriver.jar"
                ]

                ansiColor('xterm') {
                    timestamps {
                        withEnv(customEnv) {
                            timeout(time: 180, unit: 'MINUTES') {
                                code()
                            }
                        }
                    }
                }
            }
        }
    }
}

def getTestingStagesDefinition(sparkMajorVersion, config) {
    return {
        stage("Spark ${sparkMajorVersion} - ${config.backendMode}") {
            withSharedSetup(sparkMajorVersion, config) {
                withDocker(config) {
                    sh "sudo -E /usr/sbin/startup.sh"
                    prepareSparkEnvironment()(config)
                    prepareSparklingWaterEnvironment()(config)
                    buildAndLint()(config)
                    unitTests()(config)
                    pyUnitTests()(config)
                    rUnitTests()(config)
                    localIntegTest()(config)
                    localPyIntegTest()(config)
                    scriptsTest()(config)
                    pysparklingIntegTest()(config)
                }
                // Run Integration on real Hadoop Cluster
                node("dX-hadoop") {
                    ws("${env.WORKSPACE}-spark-${sparkMajorVersion}") {
                        def customEnvNew = [
                                "SPARK=spark-${config.sparkVersion}-bin-hadoop${config.hadoopVersion}",
                                "SPARK_HOME=${env.WORKSPACE}/spark",
                                "HADOOP_CONF_DIR=/etc/hadoop/conf",
                                "MASTER=yarn-client",
                                "H2O_EXTENDED_JAR=${env.WORKSPACE}/assembly-h2o/private/extended/h2odriver-extended.jar",
                                "H2O_DRIVER_JAR=${env.WORKSPACE}/assembly-h2o/private/extended/h2odriver.jar",
                                "JAVA_HOME=/usr/lib/jvm/java-8-oracle/",
                                "PATH=/usr/lib/jvm/java-8-oracle/bin:${PATH}"]
                        withEnv(customEnvNew) {
                            integTest()(config)
                        }
                    }
                }
            }
        }
    }
}

def getNightlyStageDefinition(sparkMajorVersion, config) {
    return {
        stage("Spark ${sparkMajorVersion}") {
            withSharedSetup(sparkMajorVersion, config) {
                withDocker(config) {
                    publishNightly()(config)
                }
            }
        }
    }
}

//
// Main entry point to the pipeline and definition of all stages
//

def call(params, body) {
    def config = [:]
    body.resolveStrategy = Closure.DELEGATE_FIRST

    body.delegate = config
    body(params)

    def backendTypes = []
    if (config.backendMode.toString() == "both") {
        backendTypes.add("internal")
        backendTypes.add("external")
    } else if (config.backendMode.toString() == "internal") {
        backendTypes.add("internal")
    } else {
        backendTypes.add("external")
    }

    def parallelStages = [:]
    config.sparkMajorVersions.each { version ->
        backendTypes.each { backend ->
            def configCopy = config.clone()
            configCopy["backendMode"] = backend
            parallelStages["Spark ${version} - ${backend}"] = getTestingStagesDefinition(version, configCopy)
        }
    }

    def nightlyParallelStages = [:]
    if (config.uploadNightly.toBoolean()) {
        config.sparkMajorVersions.each { version ->
            def configCopy = config.clone()
            nightlyParallelStages["Spark ${version}"] = getNightlyStageDefinition(version, configCopy)
        }
    }

    parallel(parallelStages)
    // Publish nightly only in case all tests for all Spark succeeded
    parallel(nightlyParallelStages)
}

def prepareSparkEnvironment() {
    return { config ->
        stage('Prepare Spark Environment - ' + config.backendMode) {
            sh """
                cp -R \${SPARK_HOME_${config.sparkMajorVersion.replace(".", "_")}} ${env.SPARK_HOME}
                
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
        stage('QA: Prepare Sparkling Water Environment - ' + config.backendMode) {
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
                        # Copy also original driver JAR to desired location so rest API based client can use it
                        cp ${env.H2O_ORIGINAL_JAR} ${env.H2O_DRIVER_JAR}
                    fi
                else if [ ${config.backendMode} = external ]; then
                        cp `${config.gradleCmd} -q :sparkling-water-examples:build -x check -PdoExtend extendJar -PdownloadH2O=${config.driverHadoopVersion}` ${env.H2O_EXTENDED_JAR}
                        cp `${config.gradleCmd} -q -PdoExtend -x check -PdownloadH2O=${config.driverHadoopVersion} getDownlodedJarPath` ${env.H2O_DRIVER_JAR}
                     fi
                fi
    
                """
        }
    }
}

def buildAndLint() {
    return { config ->
        stage('QA: Build and Lint - ' + config.backendMode) {
            try {
                sh "${config.gradleCmd} clean build -x check scalaStyle"
                if (config.runIntegTests.toBoolean()) {
                    stash "sw-build-${config.sparkMajorVersion}"
                }
            } finally {
                arch 'assembly/build/reports/dependency-license/**/*'
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

def pyUnitTests() {
    return { config ->
        stage('QA: Python Unit Tests 3.6 - ' + config.backendMode) {
            if (config.runPyUnitTests.toBoolean()) {
                try {
                    withCredentials([string(credentialsId: "DRIVERLESS_AI_LICENSE_KEY", variable: "DRIVERLESS_AI_LICENSE_KEY")]) {
                        sh """
                        ${config.gradleCmd} :sparkling-water-py:test -PpythonPath=/envs/h2o_env_python3.6/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -x integTest -PbackendMode=${config.backendMode}
                        """
                    }
                } finally {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr, **/build/**/*log*, **/build/reports/'
                }
            }
        }

        stage('QA: Python Unit Tests 2.7 - ' + config.backendMode) {
            if (config.runPyUnitTests.toBoolean()) {
                try {
                    withCredentials([string(credentialsId: "DRIVERLESS_AI_LICENSE_KEY", variable: "DRIVERLESS_AI_LICENSE_KEY")]) {
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

def rUnitTests() {
    return { config ->
        stage('QA: RUnit Tests - ' + config.backendMode) {
            if (config.runRUnitTests.toBoolean()) {
                try {
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

def localIntegTest() {
    return { config ->
        stage('QA: Local Integration Tests - ' + config.backendMode) {
            if (config.runLocalIntegTests.toBoolean()) {
                try {
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

def localPyIntegTest() {
    return { config ->
        stage('QA: Local Py Integration Tests 3.6 - ' + config.backendMode) {
            if (config.runLocalPyIntegTests.toBoolean()) {
                try {
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

def scriptsTest() {
    return { config ->
        stage('QA: Script Tests - ' + config.backendMode) {
            if (config.runScriptTests.toBoolean()) {
                try {
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

def integTest() {
    return { config ->
        stage('QA: Integration Tests - ' + config.backendMode) {
            if (config.runIntegTests.toBoolean()) {
                try {
                    cleanWs()
                    unstash "sw-build-${config.sparkMajorVersion}"
                    sh """
                    ${config.gradleCmd} integTest -PbackendMode=${config.backendMode} -PsparklingTestEnv=yarn -PsparkMaster=${env.MASTER} -PsparkHome=${env.SPARK_HOME} -x check -x :sparkling-water-py:integTest
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
            if (config.runPySparklingIntegTests.toBoolean()) {
                try {
                    sh """
                     ${config.gradleCmd} sparkling-water-py:yarnIntegTestsPython -PpythonPath=/envs/h2o_env_python3.6/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -PbackendMode=${config.backendMode} -PsparkHome=${env.SPARK_HOME}
                    """
                } finally {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt, **/build/reports/'
                }
            }
        }
    }
}

def publishNightly() {
    return { config ->
        stage('Nightly: Publishing Artifacts to S3 - ' + config.backendMode) {
            if (config.uploadNightly.toBoolean()) {
                withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'AWS S3 Credentials', accessKeyVariable: 'AWS_ACCESS_KEY_ID', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'],
                                 usernamePassword(credentialsId: "SIGNING_KEY", usernameVariable: 'SIGN_KEY', passwordVariable: 'SIGN_PASSWORD'),
                                 file(credentialsId: 'release-secret-key-ring-file', variable: 'RING_FILE_PATH')]) {

                    def version = getNightlyVersion(config)
                    def path = getS3Path(config)
                    sh  """
                        ${config.gradleCmd} -Pversion=${version} dist -PdoRelease -Psigning.keyId=${SIGN_KEY} -Psigning.secretKeyRingFile=${RING_FILE_PATH} -Psigning.password=
                                            
                        export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
                        export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
                        ~/.local/bin/aws s3 sync dist/build/dist s3://h2o-release/sparkling-water/spark-${config.sparkMajorVersion}/${path}${version}/ --acl public-read
                        
                        echo UPDATE LATEST POINTER
                        echo ${version} > latest
                        echo "<head>" > latest.html
                        echo "<meta http-equiv=\\"refresh\\" content=\\"0; url=${version}/index.html\\" />" >> latest.html
                        echo "</head>" >> latest.html
                        ~/.local/bin/aws s3 cp latest s3://h2o-release/sparkling-water/spark-${config.sparkMajorVersion}/${path}latest --acl public-read
                        ~/.local/bin/aws s3 cp latest.html s3://h2o-release/sparkling-water/spark-${config.sparkMajorVersion}/${path}latest.html --acl public-read
                        ~/.local/bin/aws s3 cp latest.html s3://h2o-release/sparkling-water/spark-${config.sparkMajorVersion}/${path}index.html --acl public-read                   
                        """
                }
            }
        }
    }
}

return this
