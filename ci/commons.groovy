#!/usr/bin/groovy


def dockerPull(image) {
    retryWithDelay(3, 120, {
        sh "docker pull ${image}"
    })
}

String getAWSDockerRepo() {
    def registryId = readFromInfraState("docker_registry_id")
    return "${registryId}.dkr.ecr.us-west-2.amazonaws.com"
}

def withAWSDocker(groovy.lang.Closure code) {
    docker.withRegistry("https://${getAWSDockerRepo()}", 'ecr:us-west-2:jenkins-full-aws-creds') {
        code()
    }
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
    def repoUrl = getAWSDockerRepo()
    withAWSDocker {
        def image = "${repoUrl}/opsh2oai/sparkling_water_tests:" + getDockerImageVersion()
        def dockerOptions = "--init --privileged"
        groovy.lang.Closure initCode = {
            sh "activate_java_8"
        }
        withDocker(image, code, dockerOptions, initCode)
    }
}

def withTerraform(groovy.lang.Closure code) {
    withDocker("hashicorp/terraform:0.12.24", code, "--entrypoint=''")
}

def withPacker(groovy.lang.Closure code) {
    withDocker("hashicorp/packer:1.5.5", code, "--entrypoint=''")
}

def packerBuild() {
    sh """
        packer build -var 'aws_access_key=$AWS_ACCESS_KEY_ID' -var 'aws_secret_key=$AWS_SECRET_ACCESS_KEY' Jenkins-slave-template.json
        """
    readFile("manifest.json").split("\n")
            .find() { line -> line.startsWith("      \"artifact_id\":") }
            .split("\"")[3].split(":")[1].trim()
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

def terraformImport(ami) {
    sh """
        terraform init
        terraform import -var 'aws_access_key=$AWS_ACCESS_KEY_ID' -var 'aws_secret_key=$AWS_SECRET_ACCESS_KEY' module.ami.aws_ami.jenkins-slave ${ami}
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

def withAWSCredentials(groovy.lang.Closure code) {
    withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'jenkins-full-aws-creds', accessKeyVariable: 'AWS_ACCESS_KEY_ID', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
        code()
    }
}

def withGitPushCredentials(groovy.lang.Closure code) {
    withCredentials([string(credentialsId: 'SW_GITHUB_TOKEN', variable: 'GITHUB_TOKEN')]) {
        code()
    }
}

def withGitPullCredentials(groovy.lang.Closure code) {
    withCredentials([usernamePassword(credentialsId: 'd57016f6-d172-43ea-bea1-1d6c7c1747a0', usernameVariable: 'GITHUB_PULL_USER', passwordVariable: 'GITHUB_PULL_PASS')]) {
        code()
    }
}

def withGitPrivateKey(groovy.lang.Closure code) {
    withCredentials([sshUserPrivateKey(credentialsId: 'h2oOpsGitPrivateKey', usernameVariable: 'GITHUB_SSH_USER', keyFileVariable: 'GITHUB_SSH_KEY')]) {
        code()
    }
}

def withAWSPrivateKey(groovy.lang.Closure code) {
    withCredentials([sshUserPrivateKey(credentialsId: 'SW_AWS_PRIVATE_KEY', keyFileVariable: 'AWS_SSH_KEY')]) {
        code()
    }
}

def withJenkinsCredentials(groovy.lang.Closure code) {
    withCredentials([usernamePassword(credentialsId: 'SW_JENKINS', usernameVariable: 'SW_JENKINS_USER', passwordVariable: 'SW_JENKINS_PASS')]) {
        code()
    }
}

def withDAICredentials(groovy.lang.Closure code) {
    withCredentials([string(credentialsId: 'DRIVERLESS_AI_LICENSE_KEY', variable: 'DRIVERLESS_AI_LICENSE_KEY')]) {
        code()
    }
}

def withSigningCredentials(groovy.lang.Closure code) {
    withCredentials([usernamePassword(credentialsId: 'SIGNING_KEY', usernameVariable: 'SIGN_KEY', passwordVariable: 'SIGN_PASSWORD'),
                     file(credentialsId: 'release-secret-key-ring-file', variable: 'RING_FILE_PATH')]) {
        code()
    }
}

def withNexusCredentials(groovy.lang.Closure code) {
    withCredentials([usernamePassword(credentialsId: "PUBLIC_NEXUS", usernameVariable: 'NEXUS_USERNAME', passwordVariable: 'NEXUS_PASSWORD')]) {
        code()
    }
}

def withPipyCredentials(groovy.lang.Closure code) {
    withCredentials([usernamePassword(credentialsId: "pypi-credentials", usernameVariable: 'PIPY_USERNAME', passwordVariable: 'PIPY_PASSWORD')]) {
        code()
    }
}

def withCondaCredentials(groovy.lang.Closure code) {
    withCredentials([usernamePassword(credentialsId: 'anaconda-credentials', usernameVariable: 'ANACONDA_USERNAME', passwordVariable: 'ANACONDA_PASSWORD')]) {
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
