#!/usr/bin/groovy

String getDockerImageVersion() {
    def versionLine = readFile("gradle.properties").split("\n").find() { line -> line.startsWith('dockerImageVersion') }
    return versionLine.split("=")[1]
}

def withDocker(code) {
    docker.withRegistry("http://harbor.h2o.ai") {
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
}

return this
