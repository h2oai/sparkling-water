#!/usr/bin/groovy

//
// Utility methods for the pipeline
//

def getS3Path(config) {
    return sh(script: "${getGradleCommand(config)} -q s3path", returnStdout: true).trim()
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

String getH2OBranchMajorVersion() {
    def versionLine = readFile("h2o-3/gradle.properties").split("\n").find() { line -> line.startsWith('version') }
    return versionLine.split("=")[1]
}

String getH2OBranchMajorName() {
    def versionLine = readFile("h2o-3/gradle.properties").split("\n").find() { line -> line.startsWith('codename') }
    return versionLine.split("=")[1]
}

String getH2OBranchBuildVersion() {
    return "1-SNAPSHOT"
}

def getGradleCommand(config) {
    def cmd = "${env.WORKSPACE}/gradlew -PisNightlyBuild=${config.uploadNightly} -Pspark=${config.sparkMajorVersion} -PsparkVersion=${getSparkVersion(config)} -PtestMojoPipeline=true -Dorg.gradle.internal.launcher.welcomeMessageEnabled=false"
    if (config.buildAgainstH2OBranch.toBoolean()) {
        return "H2O_HOME=${env.WORKSPACE}/h2o-3 ${cmd} -Dmaven.repo.local=${env.WORKSPACE}/.m2 -PbuildAgainstH2OBranch=${config.h2oBranch} -Ph2oMajorVersion=${getH2OBranchMajorVersion()} -Ph2oMajorName=${getH2OBranchMajorName()} -Ph2oBuild=${getH2OBranchBuildVersion()}"
    } else {
        return cmd
    }
}

def withSharedSetup(sparkMajorVersion, config, code) {
    node('docker') {
        ws("${env.WORKSPACE}-spark-${sparkMajorVersion}-${config.backendMode}") {
            try {
                config.put("sparkMajorVersion", sparkMajorVersion)
                cleanWs()
                checkout scm
                config.commons = load 'ci/commons.groovy'
                config.put("sparkVersion", getSparkVersion(config))

                if (config.buildAgainstH2OBranch.toBoolean()) {
                    config.put("driverJarPath", "${env.WORKSPACE}/h2o-3/h2o-hadoop-2/h2o-${config.driverHadoopVersion}-assembly/build/libs/h2odriver.jar")
                } else {
                    def majorVersionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('h2oMajorVersion') }
                    def majorVersion = majorVersionLine.split("=")[1]
                    def buildVersionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('h2oBuild') }
                    def buildVersion = buildVersionLine.split("=")[1]
                    config.put("driverJarPath", "${env.WORKSPACE}/.gradle/h2oDriverJars/h2odriver-${majorVersion}.${buildVersion}-${config.driverHadoopVersion}.jar")
                }
                config.put("sparkHome", "/home/jenkins/spark-${config.sparkVersion}-bin-hadoop2.7")
                def customEnv = [
                        "SPARK_HOME=${config.sparkHome}",
                        "HADOOP_CONF_DIR=/etc/hadoop/conf",
                        "H2O_DRIVER_JAR=${config.driverJarPath}"
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
            } finally {
                cleanWs()
            }
        }
    }
}

def getTestingStagesDefinition(sparkMajorVersion, config) {
    return {
        stage("Spark ${sparkMajorVersion} - ${config.backendMode}") {
            withSharedSetup(sparkMajorVersion, config) {
                config.commons.withSparklingWaterDockerImage {
                    //sh "sudo -E /usr/sbin/startup.sh"
                    buildAndLint()(config)
                    //unitTests()(config)
                    //pyUnitTests()(config)
                    //rUnitTests()(config)
                    //integTests()(config)
                    //pyIntegTests()(config)
                }
            }
        }
    }
}

def getNightlyStageDefinition(sparkMajorVersion, config) {
    return {
        stage("Spark ${sparkMajorVersion}") {
            withSharedSetup(sparkMajorVersion, config) {
                config.commons.withSparklingWaterDockerImage {
                    //publishNightly()(config)
                    publishNightlyDockerImages()(config)
                }
            }
        }
    }
}

def prepareSparklingEnvironmentStage(config) {
    stage("Prepare Sparkling Water Environment") {
        node('docker') {
            cleanWs()
            checkout scm
            pipeline = load 'ci/sparklingWaterPipeline.groovy'
            def commons = load 'ci/commons.groovy'
            commons.withSparklingWaterDockerImage {
                if (config.buildAgainstH2OBranch.toBoolean()) {
                    retryWithDelay(3, 60, {
                        sh "git clone https://github.com/h2oai/h2o-3.git"
                    })
                    retryWithDelay(5, 1, {
                        sh """
                            cd h2o-3
                            git checkout ${config.h2oBranch}
                            . /envs/h2o_env_python2.7/bin/activate
                            export BUILD_HADOOP=true
                            export H2O_TARGET=${config.driverHadoopVersion}
                            ./gradlew build --parallel -x check -Duser.name=ec2-user
                            ./gradlew publishToMavenLocal --parallel -Dmaven.repo.local=${env.WORKSPACE}/.m2 -Duser.name=ec2-user -Dhttp.socketTimeout=600000 -Dhttp.connectionTimeout=600000
                            ./gradlew :h2o-r:buildPKG -Duser.name=ec2-user
                            cd ..
                            """
                    })
                    stash name: "shared", excludes: "h2o-3/h2o-py/h2o/**/*.pyc, h2o-3/h2o-py/h2o/**/h2o.jar", includes: "h2o-3/h2o-dist/buildinfo.json, h2o-3/gradle.properties, .m2/**, h2o-3/h2o-py/h2o/**, h2o-3/h2o-r/h2o_*.99999.tar.gz, h2o-3/h2o-hadoop-2/h2o-${config.driverHadoopVersion}-assembly/build/libs/h2odriver.jar"
                } else {
                    sh "./gradlew -PhadoopDist=${config.driverHadoopVersion} downloadH2ODriverJar"
                    def majorVersionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('h2oMajorVersion') }
                    def majorVersion = majorVersionLine.split("=")[1]
                    def buildVersionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('h2oBuild') }
                    def buildVersion = buildVersionLine.split("=")[1]
                    stash name: "shared", includes: ".gradle/h2oDriverJars/h2odriver-${majorVersion}.${buildVersion}-${config.driverHadoopVersion}.jar"
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
    prepareSparklingEnvironmentStage(config)
    parallel(parallelStages)
    // Publish nightly only in case all tests for all Spark succeeded
    parallel(nightlyParallelStages)
}

def buildAndLint() {
    return { config ->
        stage('QA: Build and Lint - ' + config.backendMode) {
            try {
                unstash "shared"
                sh "${getGradleCommand(config)} clean build -x check spotlessCheck"
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
                    config.commons.withDAICredentials {
                        sh """
                            ${getGradleCommand(config)} test -x :sparkling-water-r:test -x :sparkling-water-py:test -x integTest -PbackendMode=${config.backendMode}
                            """
                    }
                } finally {
                    arch '**/build/*tests.log, **/*.log, **/out.*, **/stdout, **/stderr, **/build/**/*log*, **/build/reports/'
                    testReport 'core/build/reports/tests/test', "Spark ${config.sparkMajorVersion} ${config.backendMode} - Core Unit Tests"
                    testReport 'ml/build/reports/tests/test', "Spark ${config.sparkMajorVersion} ${config.backendMode} - ML Unit Tests"
                    testReport 'repl/build/reports/tests/test', "Spark ${config.sparkMajorVersion} ${config.backendMode} - REPL Unit Tests"
                    testReport 'utils/build/reports/tests/test', "Spark ${config.sparkMajorVersion} ${config.backendMode} - Utils Unit Tests"
                    testReport 'macros/build/reports/tests/test', "Spark ${config.sparkMajorVersion} ${config.backendMode} - Macros Unit Tests"
                }
            }
        }
    }
}

def pyUnitTests() {
    return { config ->
        stage('QA: PyUnit Tests 3.6 - ' + config.backendMode) {
            if (config.runPyUnitTests.toBoolean()) {
                try {
                    config.commons.withDAICredentials {
                        sh """
                            ${getGradleCommand(config)} :sparkling-water-py:test -PpythonPath=/envs/h2o_env_python3.6/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -x integTest -PbackendMode=${config.backendMode}
                            """
                    }
                } finally {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr, **/build/**/*log*, **/build/reports/'
                }
            }
        }

        stage('QA: PyUnit Tests 2.7 - ' + config.backendMode) {
            if (config.runPyUnitTests.toBoolean()) {
                try {
                    config.commons.withDAICredentials {
                        sh """
                            ${getGradleCommand(config)} :sparkling-water-py:test -PpythonPath=/envs/h2o_env_python2.7/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -x integTest -PbackendMode=${config.backendMode}
                            """
                    }
                } finally {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr, **/build/**/*log*, **/build/reports/'
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
                    if (config.buildAgainstH2OBranch.toBoolean()) {
                        sh """
                                R -e 'install.packages("h2o-3/h2o-r/h2o_${getH2OBranchMajorVersion()}.99999.tar.gz", type="source", repos=NULL)'
                            """
                    } else {
                        sh """
                            ${getGradleCommand(config)} :sparkling-water-r:installH2ORPackage
                            """
                    }
                    sh """
                         ${getGradleCommand(config)} :sparkling-water-r:installRSparklingPackage
                         ${getGradleCommand(config)} :sparkling-water-r:test -x check -PbackendMode=${config.backendMode}
                         """
                } finally {
                    arch '**/build/*tests.log,**/*.log, **/out.*, **/stdout, **/stderr, **/build/**/*log*, **/build/reports/'
                }
            }
        }
    }
}

def integTests() {
    return { config ->
        stage('QA: Integration Tests - ' + config.backendMode) {
            if (config.runIntegTests.toBoolean()) {
                try {
                    sh """
                    ${getGradleCommand(config)} integTest -x :sparkling-water-py:integTest -PsparkHome=${env.SPARK_HOME} -PbackendMode=${config.backendMode}
                    """
                } finally {
                    arch '**/build/*tests.log, **/*.log, **/out.*, **/stdout, **/stderr, **/build/**/*log*, **/build/reports/'
                    testReport 'core/build/reports/tests/integTest', "Spark ${config.sparkMajorVersion} ${config.backendMode} - Core Integration Tests"
                    testReport 'examples/build/reports/tests/integTest', "Spark ${config.sparkMajorVersion} ${config.backendMode} - Examples Integration Tests"
                    testReport 'ml/build/reports/tests/integTest', "Spark ${config.sparkMajorVersion} ${config.backendMode} - ML Integration Tests"
                }
            }
        }
    }
}

def pyIntegTests() {
    return { config ->
        stage('QA: Py Integration Tests 3.6 - ' + config.backendMode) {
            if (config.runPyIntegTests.toBoolean()) {
                try {
                    sh """
                    ${getGradleCommand(config)} sparkling-water-py:integTest -PpythonPath=/envs/h2o_env_python3.6/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -PsparkHome=${env.SPARK_HOME} -PbackendMode=${config.backendMode}
                    """
                } finally {
                    arch '**/build/*tests.log, **/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr,**/build/**/*log*, py/build/py_*_report.txt,**/build/reports/'
                }
            }
        }
    }
}

def publishNightly() {
    return { config ->
        stage('Nightly: Publishing Artifacts to S3 - ' + config.backendMode) {
            if (config.uploadNightly.toBoolean()) {
                config.commons.withAWSCredentials {
                    config.commons.withSigningCredentials {
                        unstash "shared"
                        def version = getNightlyVersion(config)
                        def path = getS3Path(config)
                        sh """
                            sed -i 's/^version=.*\$/version=${version}/' gradle.properties
                            echo "doRelease=true" >> gradle.properties
                            ${getGradleCommand(config)} dist -Psigning.keyId=${SIGN_KEY} -Psigning.secretKeyRingFile=${RING_FILE_PATH} -Psigning.password=

                            export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
                            export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
                            ~/.local/bin/aws s3 sync dist/build/dist "s3://h2o-release/sparkling-water/spark-${config.sparkMajorVersion}/${path}${version}" --acl public-read

                            echo UPDATE LATEST POINTER
                            echo ${version} > latest
                            echo "<head>" > latest.html
                            echo "<meta http-equiv=\\"refresh\\" content=\\"0; url=${version}/index.html\\" />" >> latest.html
                            echo "</head>" >> latest.html
                            ~/.local/bin/aws s3 cp latest "s3://h2o-release/sparkling-water/spark-${config.sparkMajorVersion}/${path}latest" --acl public-read
                            ~/.local/bin/aws s3 cp latest.html "s3://h2o-release/sparkling-water/spark-${config.sparkMajorVersion}/${path}latest.html" --acl public-read
                            ~/.local/bin/aws s3 cp latest.html "s3://h2o-release/sparkling-water/spark-${config.sparkMajorVersion}/${path}index.html" --acl public-read
                            """
                    }
                }
            }
        }
    }
}

def publishSparklingWaterDockerImage(String type, version, sparkMajorVersion) {
    sh """
        ./bin/build-kubernetes-images.sh ${type}
        docker tag sparkling-water-${type}:${version} distrace/sparkling-water-${type}:latest-nightly-${sparkMajorVersion}
        docker push distrace/sparkling-water-${type}:latest-nightly-${sparkMajorVersion}
        docker rmi distrace/sparkling-water-${type}:latest-nightly-${sparkMajorVersion}
    """
}

def publishNightlyDockerImages() {
    return { config ->
        stage('Publish to Docker Hub') {
            if (config.uploadNightlyDockerImages.toBoolean()) {
                config.commons.withSigningCredentials {
                    unstash "shared"
                    sh "sudo apt -y install docker.io"
                    sh "sudo service docker start"
                    sh "sudo chmod 666 /var/run/docker.sock"
                    def version = getNightlyVersion(config)
                    sh """
                        sed -i 's/^version=.*\$/version=${version}/' gradle.properties
                        echo "doRelease=true" >> gradle.properties
                        ${getGradleCommand(config)} dist -Psigning.keyId=${SIGN_KEY} -Psigning.secretKeyRingFile=${RING_FILE_PATH} -Psigning.password=
                       """
                    params.commons.withDockerHubCredentials {
                        docker.withRegistry('', 'dockerhub') {
                            dir("./dist/build/zip/sparkling-water-${version}") {
                                publishSparklingWaterDockerImage("scala", version, config.sparkMajorVersion)
                                publishSparklingWaterDockerImage("r", version, config.sparkMajorVersion)
                                publishSparklingWaterDockerImage("python", version, config.sparkMajorVersion)
                                publishSparklingWaterDockerImage("external-backend", version, config.sparkMajorVersion)
                            }
                        }
                    }
                }
            }
        }
    }
}

return this
