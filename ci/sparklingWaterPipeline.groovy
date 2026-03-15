#!/usr/bin/groovy

//
// Utility methods for the pipeline
//

def getDriverHadoopVersion() {
    return "cdh6.3"
}

def getHadoopMajorVersion() {
    return "3"
}

def getS3Path(config) {
    return sh(script: "${getGradleCommand(config)} -q s3path", returnStdout: true).trim()
}

String getVersion(config) {
    return readFile("gradle.properties").split("\n").find() { line -> line.startsWith("version") }.split("=")[1]
}

String getNightlyVersion(config) {
    def sparkMajorVersion = config.sparkMajorVersion
    def version = getVersion(config)
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
    return "bleeding_edge"
}

String getH2OBranchBuildVersion() {
    return "1-SNAPSHOT"
}

def getGradleCommand(config) {
    String maybeDebug = params.gradleDebug ? '--debug ' : ''
    def cmd = "${env.WORKSPACE}/gradlew ${maybeDebug}-PisNightlyBuild=${config.uploadNightly} -Pspark=${config.sparkMajorVersion} -PsparkVersion=${getSparkVersion(config)} -PtestMojoPipeline=true -Dorg.gradle.internal.launcher.welcomeMessageEnabled=false"
    if (config.buildAgainstH2OBranch.toBoolean()) {
        return "H2O_HOME=${env.WORKSPACE}/h2o-3 ${cmd} -Dmaven.repo.local=${env.WORKSPACE}/.m2 -PbuildAgainstH2OBranch=${config.h2oBranch} -Ph2oMajorVersion=${getH2OBranchMajorVersion()} -Ph2oMajorName=${getH2OBranchMajorName()} -Ph2oBuild=${getH2OBranchBuildVersion()}"
    } else {
        return cmd
    }
}

def buildSpark35Distribution() {
    buildSparkDistribution("3.5", "branch-3.5")
}

/**
 * Build Apache Spark from source with CVE-fixing dependency overrides.
 *
 * @param sparkMajorVersion  e.g. "3.5" or "4.1"
 * @param sparkBranch        git branch or tag, e.g. "branch-3.5" or "v4.1.1"
 */
def buildSparkDistribution(sparkMajorVersion, sparkBranch) {
    stage("Build Spark ${sparkMajorVersion} (security overrides)") {
        sh """
            rm -rf spark
            if echo "${sparkBranch}" | grep -qE '^(v)?[0-9]+\\.[0-9]+\\.[0-9]+\$'; then
                git clone --branch "${sparkBranch}" https://github.com/apache/spark.git spark
            else
                git clone --depth 1 --branch "${sparkBranch}" https://github.com/apache/spark.git spark
            fi

            cat > spark/maven-settings.xml <<'XMLEOF'
<settings>
  <mirrors>
    <mirror>
      <id>maven-central</id>
      <name>Maven Central</name>
      <url>https://repo.maven.apache.org/maven2</url>
      <mirrorOf>central</mirrorOf>
    </mirror>
  </mirrors>
</settings>
XMLEOF

            # --- CVE patches applied via sed (all Spark versions) ---
            # commons-beanutils 1.9.4 -> 1.11.0 (CVE-2025-48734)
            sed -i.bak -e 's/<artifactId>commons-beanutils<\\/artifactId>\\n        <version>1.9.4<\\/version>/<artifactId>commons-beanutils<\\/artifactId>\\n        <version>1.11.0<\\/version>/' spark/pom.xml

            # lz4-java: swap to at.yawk.lz4 fork (CVE-2025-66566)
            sed -i.bak -e '/<groupId>org.lz4<\\/groupId>/,/<\\/dependency>/{ s/<groupId>org\\.lz4<\\/groupId>/<groupId>at.yawk.lz4<\\/groupId>/; s/<version>1\\.8\\.0<\\/version>/<version>1.10.1<\\/version>/; s/<version>1\\.8\\.1<\\/version>/<version>1.10.1<\\/version>/; }' spark/pom.xml
            sed -i.bak -e '/<groupId>org.lz4<\\/groupId>/,/<\\/dependency>/s/<groupId>org\\.lz4<\\/groupId>/<groupId>at.yawk.lz4<\\/groupId>/' spark/core/pom.xml || true
            sed -i.bak -e '/<groupId>org.lz4<\\/groupId>/,/<\\/dependency>/s/<groupId>org\\.lz4<\\/groupId>/<groupId>at.yawk.lz4<\\/groupId>/' spark/connector/kafka-0-10-assembly/pom.xml || true

            # Ensure Guava failureaccess is shaded alongside Guava
            sed -i.bak -e '/<include>com.google.guava:guava<\\/include>/a\\              <include>com.google.guava:failureaccess<\\/include>' spark/pom.xml
            sed -i.bak -e '/<include>com.google.guava:guava<\\/include>/a\\              <include>com.google.guava:failureaccess<\\/include>' spark/core/pom.xml || true

            # aircompressor 2.0.2 -> 2.0.3 (CVE-2025-67721)
            sed -i.bak -e '/<artifactId>aircompressor<\\/artifactId>/,/<version>/s/<version>2\\.0\\.2<\\/version>/<version>2.0.3<\\/version>/' spark/pom.xml

            cd spark
            export MAVEN_OPTS="-Xss128m -Xmx8g -XX:ReservedCodeCacheSize=2g"
            """

        def isSpark4 = sparkMajorVersion.startsWith("4")

        // Version-specific Maven overrides:
        // - Spark 3.x: Scala 2.12, Hadoop 3.3.6, Jetty 9.x, Netty, Protobuf overrides needed
        // - Spark 4.x: Scala 2.13 only, Hadoop 3.4.2, no Jetty (uses Jakarta), ships newer Netty/Protobuf
        def scalaProfile = isSpark4 ? "" : "-Pscala-2.12"
        def hadoopVersion = isSpark4 ? "3.4.2" : "3.3.6"
        def spark3Overrides = isSpark4 ? "" : "-Djetty.version=9.4.57.v20241219 -Dnetty.version=4.1.125.Final -Dprotobuf.version=3.25.5"

        sh """
            cd spark
            ./dev/make-distribution.sh --tgz -Phadoop-3 ${scalaProfile} -Pkubernetes -Dmaven.test.skip=true \\
              -Dhadoop.version=${hadoopVersion} ${spark3Overrides} -Divy.version=2.5.2 \\
              -Dguava.version=32.0.1-jre \\
              -Dlibthrift.version=0.14.0 -Dzookeeper.version=3.7.2 \\
              -Dcommons-compress.version=1.26.0 \\
              -Dsnappy.version=1.1.10.5 \\
              -Dcommons-codec.version=1.17.0 \\
              -s ./maven-settings.xml -U

            mkdir -p ${env.WORKSPACE}/spark-dist
            TGZ_FILE="\$(ls -1 spark-*-bin-*.tgz 2>/dev/null | head -n 1)"
            if [ -z "\${TGZ_FILE}" ]; then
                TGZ_FILE="\$(ls -1 dist/*.tgz 2>/dev/null | head -n 1)"
            fi
            if [ -z "\${TGZ_FILE}" ]; then
                echo "ERROR: Spark distribution tgz not found"
                exit 1
            fi
            mv "\${TGZ_FILE}" ${env.WORKSPACE}/spark-dist/spark-${sparkMajorVersion}.tgz
            """
        stash name: "spark-${sparkMajorVersion}-dist", includes: "spark-dist/spark-${sparkMajorVersion}.tgz"
    }
}

/**
 * Build the hardened Docker image using kubernetes/Dockerfile-Hardened.
 * Prepares the build context with Spark JARs, assembly JAR, PySparkling,
 * and applies fabric8/Jackson cleanup before building.
 *
 * @param config  pipeline config map with sparkMajorVersion, sparkVersion
 * @return the Docker image name:tag
 */
def buildHardenedDockerImage(config) {
    def sparkMajorVersion = config.sparkMajorVersion
    def sparkVersion = config.sparkVersion ?: getSparkVersion(config)
    def version = getVersion(config)
    def scalaBaseVersion = readFile("gradle-spark${sparkMajorVersion}.properties")
        .split("\n").find { it.startsWith("scalaVersion") }.split("=")[1]
        .split("\\.")[0..1].join(".")
    def imageName = "sparkling-water-hardened:${version}-spark-${sparkMajorVersion}"

    stage("Build Hardened Docker Image (Spark ${sparkMajorVersion})") {
        unstash "spark-${sparkMajorVersion}-dist"

        sh """#!/bin/bash
set -Eeuo pipefail

rm -rf docker-build-context spark-jars-temp
mkdir -p docker-build-context/spark-jars docker-build-context/spark-python \
         docker-build-context/spark-bin docker-build-context/spark-sbin \
         docker-build-context/spark-entrypoint spark-jars-temp

tar -xzf spark-dist/spark-${sparkMajorVersion}.tgz -C spark-jars-temp --strip-components=1

# Copy Spark JARs
cp spark-jars-temp/jars/*.jar docker-build-context/spark-jars/

# Remove Spark's fabric8 6.x JARs so the assembly's 7.x version is used
cd docker-build-context/spark-jars
rm -fv kubernetes-client*.jar kubernetes-model*.jar volcano*.jar 2>/dev/null || true

# Download hadoop-aws + AWS SDK bundle if missing
HADOOP_VERSION="3.4.2"
if [ -z "\$(ls hadoop-aws-*.jar 2>/dev/null)" ]; then
    curl -fL -o "hadoop-aws-\${HADOOP_VERSION}.jar" \\
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/\${HADOOP_VERSION}/hadoop-aws-\${HADOOP_VERSION}.jar"
fi
if [ -z "\$(ls aws-java-sdk-bundle-*.jar 2>/dev/null)" ]; then
    curl -fL -o aws-java-sdk-bundle-1.12.797.jar \\
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.797/aws-java-sdk-bundle-1.12.797.jar"
fi
if [ -z "\$(ls bundle-*.jar 2>/dev/null)" ]; then
    curl -fL -o bundle-2.34.0.jar \\
        "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.34.0/bundle-2.34.0.jar"
fi
cd ${env.WORKSPACE}

# Spark python files
if [ -d spark-jars-temp/python ]; then
    cp -r spark-jars-temp/python/* docker-build-context/spark-python/ 2>/dev/null || true
fi

# Spark bin (required for K8s)
cp -r spark-jars-temp/bin/* docker-build-context/spark-bin/
chmod +x docker-build-context/spark-bin/* || true

# Spark sbin
if [ -d spark-jars-temp/sbin ]; then
    cp -r spark-jars-temp/sbin/* docker-build-context/spark-sbin/ 2>/dev/null || true
fi

# Spark K8s entrypoint
if [ -f spark-jars-temp/kubernetes/dockerfiles/spark/entrypoint.sh ]; then
    cp spark-jars-temp/kubernetes/dockerfiles/spark/entrypoint.sh docker-build-context/spark-entrypoint/
elif [ -f spark-jars-temp/bin/entrypoint.sh ]; then
    cp spark-jars-temp/bin/entrypoint.sh docker-build-context/spark-entrypoint/
else
    echo "ERROR: entrypoint.sh not found in Spark distribution"
    find spark-jars-temp -name "entrypoint.sh" 2>/dev/null || true
    exit 1
fi
chmod +x docker-build-context/spark-entrypoint/entrypoint.sh

# Copy Sparkling Water assembly JAR
SW_ASSEMBLY_JAR=\$(ls assembly/build/libs/sparkling-water-assembly_*-all.jar 2>/dev/null | head -n 1)
if [ -z "\${SW_ASSEMBLY_JAR}" ]; then
    echo "ERROR: Assembly JAR not found"
    exit 1
fi
cp "\${SW_ASSEMBLY_JAR}" docker-build-context/sparkling-water-assembly.jar

# Copy PySparkling zip
PY_ZIP=\$(ls py/build/dist/h2o_pysparkling_*.zip 2>/dev/null | head -n 1)
if [ -z "\${PY_ZIP}" ]; then
    echo "ERROR: PySparkling zip not found"
    exit 1
fi
cp "\${PY_ZIP}" "docker-build-context/\$(basename "\${PY_ZIP}")"

# Build the hardened image
docker build --pull --progress=plain \\
    --build-arg HADOOP_VERSION=3.4.2 \\
    --build-arg PYSPARK_VERSION=${sparkVersion} \\
    -t ${imageName} \\
    -f kubernetes/Dockerfile-Hardened \\
    docker-build-context

rm -rf docker-build-context spark-jars-temp
"""
    }
    return imageName
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
                def kubernetesBoundaryVersionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('kubernetesSupportSinceSpark') }
                def kubernetesBoundaryVersion = kubernetesBoundaryVersionLine.split("=")[1]
                config.put("kubernetesSupported", config.commons.isKubernetesSupported(kubernetesBoundaryVersion, sparkMajorVersion))
                def sparkDistStash = "spark-${config.sparkMajorVersion}-dist"
                def sparkDistFile = "spark-dist/spark-${config.sparkMajorVersion}.tgz"
                try {
                    unstash sparkDistStash
                    sh """
                        rm -rf spark-home
                        mkdir -p spark-home
                        tar -xzf ${sparkDistFile} -C spark-home --strip-components=1
                        """
                    config.put("sparkHome", "${env.WORKSPACE}/spark-home")
                } catch (Exception ignored) {
                    config.put("sparkHome", "/home/jenkins/spark-${config.sparkVersion}-bin")
                }
                if (config.buildAgainstH2OBranch.toBoolean()) {
                    config.put("driverJarPath", "${env.WORKSPACE}/h2o-3/h2o-hadoop-${getHadoopMajorVersion()}/h2o-${getDriverHadoopVersion()}-assembly/build/libs/h2odriver.jar")
                } else {
                    def majorVersionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('h2oMajorVersion') }
                    def majorVersion = majorVersionLine.split("=")[1]
                    def buildVersionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('h2oBuild') }
                    def buildVersion = buildVersionLine.split("=")[1]
                    config.put("driverJarPath", "${env.WORKSPACE}/.gradle/h2oDriverJars/h2odriver-${majorVersion}.${buildVersion}-${getDriverHadoopVersion()}.jar")
                }
                def customEnv = [
                        "SPARK_HOME=${config.sparkHome}",
                        "HADOOP_CONF_DIR=/etc/hadoop/conf",
                        "H2O_DRIVER_JAR=${config.driverJarPath}"
                ]

                ansiColor('xterm') {
                    timestamps {
                        withEnv(customEnv) {
                            timeout(time: 600, unit: 'MINUTES') {
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
                    sh "sudo -E /usr/sbin/startup.sh"
                    buildAndLint()(config)
                    unitTests()(config)
                    pyUnitTests()(config)
                    rUnitTests()(config)
                    integTests()(config)
                    pyIntegTests()(config)
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
                    config.commons.withSigningCredentials {
                        unstash "shared"
                        def version = getNightlyVersion(config)
                        sh """
                            sed -i 's/^version=.*\$/version=${version}/' gradle.properties
                            sed -i 's/^h2oMajorName=.*\$/h2oMajorName=${getH2OBranchMajorName()}/' gradle.properties
                            sed -i 's/^h2oMajorVersion=.*\$/h2oMajorVersion=${getH2OBranchMajorVersion()}/' gradle.properties
                            sed -i 's/^h2oBuild=.*\$/h2oBuild=${getH2OBranchBuildVersion()}/' gradle.properties
                            echo "doRelease=true" >> gradle.properties
                            """
                        sh "${getGradleCommand(config)} dist -PmakeBooklet=true -Psigning.keyId=${SIGN_KEY} -Psigning.secretKeyRingFile=${RING_FILE_PATH} -Psigning.password="
                        publishNightly()(config)
                        publishNightlyDockerImages()(config)
                    }
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
                config.sparkMajorVersions.each { sparkMajorVersion ->
                    if (sparkMajorVersion.startsWith("4.")) {
                        def sparkPropsFile = "gradle-spark${sparkMajorVersion}.properties"
                        def sparkVersionLine = readFile(sparkPropsFile).split("\n").find { it.startsWith("sparkVersion") }
                        def sparkVersion = sparkVersionLine.split("=")[1]
                        buildSparkDistribution(sparkMajorVersion, "v${sparkVersion}")
                    } else if (sparkMajorVersion == "3.5") {
                        buildSparkDistribution("3.5", "branch-3.5")
                    }
                }
                if (config.buildAgainstH2OBranch.toBoolean()) {
                    retryWithDelay(3, 60, {
                        sh "git clone https://github.com/h2oai/h2o-3.git"
                    })
                    retryWithDelay(5, 1, {
                        sh """
                            cd h2o-3
                            git checkout ${config.h2oBranch}
                            . /envs/h2o_env_python3.6/bin/activate
                            unset CI
                            export BUILD_HADOOP=true
                            export H2O_TARGET=${getDriverHadoopVersion()}
                            ./gradlew build --parallel -x check -Duser.name=ec2-user
                            ./gradlew publishToMavenLocal --parallel -Dmaven.repo.local=${env.WORKSPACE}/.m2 -Duser.name=ec2-user -Dhttp.socketTimeout=600000 -Dhttp.connectionTimeout=600000
                            ./gradlew :h2o-r:buildPKG -Duser.name=ec2-user
                            cd ..
                            """
                    })
                    stash name: "shared", excludes: "h2o-3/h2o-py/h2o/**/*.pyc, h2o-3/h2o-py/h2o/**/h2o.jar", includes: "h2o-3/build/h2o.jar, h2o-3/h2o-dist/buildinfo.json, h2o-3/gradle.properties, .m2/**, h2o-3/h2o-py/h2o/**, h2o-3/h2o-r/h2o_*.99999.tar.gz, h2o-3/h2o-hadoop-${getHadoopMajorVersion()}/h2o-${getDriverHadoopVersion()}-assembly/build/libs/h2odriver.jar"
                } else {
                    sh "./gradlew -PhadoopDist=${getDriverHadoopVersion()} downloadH2ODriverJar"
                    def majorVersionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('h2oMajorVersion') }
                    def majorVersion = majorVersionLine.split("=")[1]
                    def buildVersionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('h2oBuild') }
                    def buildVersion = buildVersionLine.split("=")[1]
                    stash name: "shared", includes: ".gradle/h2oDriverJars/h2odriver-${majorVersion}.${buildVersion}-${getDriverHadoopVersion()}.jar"
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
                            ${getGradleCommand(config)} test -x :sparkling-water-r:test -x :sparkling-water-py:test -x :sparkling-water-py-scoring:test -x integTest -PsparkHome=${env.SPARK_HOME} -PbackendMode=${config.backendMode}
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
        def allPythonVersions = config.commons.getSupportedPythonVersions(config.sparkMajorVersion)
        def pythonVersions
        if (config.runUnitTestsForAllPythonVersions.toBoolean()) {
            pythonVersions = allPythonVersions
        } else {
            pythonVersions = [allPythonVersions.first(), allPythonVersions.last()]
        }
        for (pythonProject in ["py", "py-scoring"]) {
            for (pythonVersion in pythonVersions) {
                stage("QA: PyUnit Tests ${pythonVersion} - ${pythonProject} - ${config.backendMode}") {
                    if (config.runPyUnitTests.toBoolean()) {
                        try {
                            config.commons.withDAICredentials {
                                sh "${getGradleCommand(config)} :sparkling-water-${pythonProject}:test " +
                                   "-PpythonPath=/home/jenkins/miniconda/envs/sw_env_python${pythonVersion}/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -x integTest -PsparkHome=${env.SPARK_HOME} -PbackendMode=${config.backendMode}"
                            }
                        } finally {
                            arch '**/build/*tests.log,**/*.log, **/out.*, **/*py.out.txt, **/stdout, **/stderr, **/build/**/*log*, **/build/reports/'
                        }
                    }
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
                    sh """R -e 'dir.create(Sys.getenv("R_LIBS_USER"), recursive = TRUE)'"""
                    if (config.buildAgainstH2OBranch.toBoolean()) {
                        sh """
                            R -e 'install.packages("h2o-3/h2o-r/h2o_${getH2OBranchMajorVersion()}.99999.tar.gz", type="source", repos=NULL)'
                            """
                    } else {
                        sh """
                            ${getGradleCommand(config)} :sparkling-water-r:installH2ORPackage
                            """
                    }
                    sh "${getGradleCommand(config)} :sparkling-water-r:installRSparklingPackage"
                    config.commons.withDAICredentials {
                        timeout(time: 7, unit: 'MINUTES') {
                            sh """
                                unset MASTER
                                ${getGradleCommand(config)} :sparkling-water-r:test -x check -PbackendMode=${config.backendMode}
                                """
                        }
                    }
                } catch (org.jenkinsci.plugins.workflow.steps.FlowInterruptedException e){
                    currentBuild.result = "SUCCESS"
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
                    echo 'jenkins:jenkins' | sudo chpasswd
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
        def allPythonVersions = config.commons.getSupportedPythonVersions(config.sparkMajorVersion)
        def pythonVersion = allPythonVersions.last()
        stage("QA: Py Integration Tests ${pythonVersion} - ${config.backendMode}") {
            if (config.runPyIntegTests.toBoolean()) {
                try {
                    sh """
                    echo 'jenkins:jenkins' | sudo chpasswd
                    ${getGradleCommand(config)} sparkling-water-py:integTest -PpythonPath=/home/jenkins/miniconda/envs/sw_env_python${pythonVersion}/bin -PpythonEnvBasePath=/home/jenkins/.gradle/python -PsparkHome=${env.SPARK_HOME} -PbackendMode=${config.backendMode}
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
                config.commons.withRootAWSCredentials {
                    def version = getVersion(config)
                    def path = getS3Path(config)
                    sh """
                        export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
                        export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
                        ~/.local/bin/aws s3 sync dist/build/dist "s3://h2o-release/sparkling-water/spark-${config.sparkMajorVersion}/${path}${version}" --acl public-read
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

def publishSparklingWaterDockerImage(String type, version, sparkMajorVersion) {
    def branchName = env.BRANCH_NAME
    sh """
        H2O_HOME=${env.WORKSPACE}/h2o-3 ./bin/build-kubernetes-images.sh ${type}
        docker tag sparkling-water-${type}:${version} h2oai/sparkling-water-${type}:latest-nightly-${branchName}-${sparkMajorVersion}
        docker push h2oai/sparkling-water-${type}:latest-nightly-${branchName}-${sparkMajorVersion}
        docker rmi h2oai/sparkling-water-${type}:latest-nightly-${branchName}-${sparkMajorVersion}
        docker rmi sparkling-water-${type}:${version}
    """
}

def publishNightlyDockerImages() {
    return { config ->
        if (config.kubernetesSupported.toBoolean()) {
            stage('Publish to Docker Hub') {
                if (config.uploadNightlyDockerImages.toBoolean()) {
                        config.commons.installDocker()
                        def version = getVersion(config)
                        def sparkVersion = getSparkVersion(config)
                        config.commons.publishDockerImages(version) {
                            publishSparklingWaterDockerImage("scala", version, config.sparkMajorVersion)
                            publishSparklingWaterDockerImage("r", version, config.sparkMajorVersion)
                            publishSparklingWaterDockerImage("python", version, config.sparkMajorVersion)
                            config.commons.removeSparkImages(sparkVersion)
                            publishSparklingWaterDockerImage("external-backend", version, config.sparkMajorVersion)
                    }
                }
            }
        }
    }
}

return this
