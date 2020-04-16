#!/usr/bin/groovy


def dockerPull(image) {
    retryWithDelay(3, 120, {
        sh "docker pull ${image}"
    })
}

def internalH2ODockerLogin() {
    retryWithDelay(3, 120, {
        withCredentials([usernamePassword(credentialsId: "harbor.h2o.ai", usernameVariable: 'REGISTRY_USERNAME', passwordVariable: 'REGISTRY_PASSWORD')]) {
            sh "docker login -u $REGISTRY_USERNAME -p $REGISTRY_PASSWORD harbor.h2o.ai"
        }
    })
}

Integer getDockerImageVersion() {
    def versionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('dockerImageVersion') }
    return versionLine.split("=")[1].toInteger()
}

def getSupportedSparkVersions() {
    def versionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('supportedSparkVersions') }
    sparkVersions = versionLine.split("=")[1].split(" ")

    if (isPrJob()) {
        // test PRs only on first and last supported Spark
        sparkVersions = [sparkVersions.first(), sparkVersions.last()]
    }

    return sparkVersions
}

def withDocker(image, groovy.lang.Closure code, String dockerOptions = "", groovy.lang.Closure initCode = {}) {
    dockerPull(image)
    docker.image(image).inside(dockerOptions) {
        initCode()
        code()
    }
}

def withSparklingWaterDockerImage(code) {
    docker.withRegistry("http://harbor.h2o.ai") {
        internalH2ODockerLogin()
        def image = "harbor.h2o.ai/opsh2oai/sparkling_water_tests:" + getDockerImageVersion()
        def dockerOptions = "--init --privileged --dns 172.16.0.200"
        groovy.lang.Closure initCode = {
            sh "activate_java_8"
        }
        withDocker(image, code, dockerOptions, initCode)
    }
}

def withAWSCLI(groovy.lang.Closure code) {
    withDocker("harbor.h2o.ai/opsh2oai/awscli", code, "--entrypoint=''")
}

def withTerraform(groovy.lang.Closure code) {
    withDocker("hashicorp/terraform:0.12.24", code, "--entrypoint=''")
}

def terraformApply(extraVars = "") {
    sh """
        terraform init
        terraform apply -var aws_access_key=$AWS_ACCESS_KEY_ID -var aws_secret_key=$AWS_SECRET_ACCESS_KEY $extraVars -auto-approve
        """
}

def terraformDestroy() {
    sh """
        terraform init
        terraform destroy -var aws_access_key=$AWS_ACCESS_KEY_ID -var aws_secret_key=$AWS_SECRET_ACCESS_KEY -auto-approve
        """
}

def extractTerraformOutputs(List<String> varNames) {
    return varNames.collectEntries { [(it): terraformOutput(it)] }
}

def readFromInfraState(varName) {
    def line = readFile("ci/aws/terraform/infra.properties").split("\n").find() { line ->
        line.startsWith(varName)
    }
    return line.split("=")[1]
}

def terraformOutput(varName) {
    return sh(
            script: "terraform output $varName",
            returnStdout: true
    ).trim()
}

def withAWSCredentials(code) {
    withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'AWS S3 Credentials', accessKeyVariable: 'AWS_ACCESS_KEY_ID', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
        code()
    }
}

def withGitPushCredentials(code) {
    withCredentials([string(credentialsId: 'SW_GITHUB_TOKEN', variable: 'GITHUB_TOKEN')]) {
        code()
    }
}

def withGitPullCredentials(code) {
    withCredentials([usernamePassword(credentialsId: 'd57016f6-d172-43ea-bea1-1d6c7c1747a0', usernameVariable: 'GITHUB_PULL_USER', passwordVariable: 'GITHUB_PULL_PASS')]) {
        code()
    }
}

def withJenkinsCredentials(code) {
    withCredentials([usernamePassword(credentialsId: 'SW_JENKINS', usernameVariable: 'SW_JENKINS_USER', passwordVariable: 'SW_JENKINS_PASS')]) {
        code()
    }
}

def withDAICredentials(code) {
    withCredentials([string(credentialsId: 'DRIVERLESS_AI_LICENSE_KEY', variable: 'DRIVERLESS_AI_LICENSE_KEY')]) {
        code()
    }
}

def withSigningCredentials(code) {
    withCredentials([usernamePassword(credentialsId: 'SIGNING_KEY', usernameVariable: 'SIGN_KEY', passwordVariable: 'SIGN_PASSWORD'),
                     file(credentialsId: 'release-secret-key-ring-file', variable: 'RING_FILE_PATH')]) {
        code()
    }
}

def gitCommit(files, msg) {
    sh """
                git config --add remote.origin.fetch +refs/heads/${BRANCH_NAME}:refs/remotes/origin/${BRANCH_NAME}
                git fetch --no-tags
                git checkout ${BRANCH_NAME}
                git pull
                git add ${files.join(" ")}
                git config remote.origin.url "https://${GITHUB_TOKEN}@github.com/h2oai/sparkling-water.git"
                git commit -m "${msg}"
                git push --set-upstream origin ${BRANCH_NAME}
               """
}

return this
