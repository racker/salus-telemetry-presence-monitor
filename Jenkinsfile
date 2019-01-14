def label = "maven-${UUID.randomUUID().toString()}"

podTemplate(label: label, containers: [
  containerTemplate(name: 'maven', image: 'maven:3-jdk-8', ttyEnabled: true, command: 'cat'),
  containerTemplate(name: 'gcloud', image: 'google/cloud-sdk', ttyEnabled: true, command: 'cat')
  ])
{
    node(label) {
        container('maven') {
            ansiColor('xterm') {
                stage('Checkout') {
                    checkout scm
                    googleStorageDownload bucketUri: 'gs://monplat-jenkins-artifacts/settings.xml', credentialsId: 'monplat-jenkins', localDirectory: './.mvn/'
                }
                stage('Maven install') {
                  sh 'mvn install -Dmaven.test.skip=true -s .mvn/settings.xml'
                }
                stage('Integration Test') {
                  // sh 'mvn integration-test'
                  sh 'TZ=":America/Chicago" date'
                }
                stage('Deploy snapshot') {
                  sh 'mvn deploy -Dmaven.test.skip=true -s .mvn/settings.xml'
                }
            }
        }
        container('gcloud') {
            stage('Deploy docker') {
                withCredentials([[$class: 'FileBinding', credentialsId: 'salus-dev-gcr', variable: 'GOOGLE_APPLICATION_CREDENTIALS']]) {
                    sh 'gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS'
                    sh 'curl -fsSL "https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v1.5.0/docker-credential-gcr_linux_amd64-1.5.0.tar.gz" | tar xz --to-stdout ./docker-credential-gcr > /usr/bin/docker-credential-gcr'
                    sh 'chmod +x /usr/bin/docker-credential-gcr'
                    sh 'docker-credential-gcr configure-docker'
                    sh './mvnw -P docker -Dmaven.test.skip=true -Dmaven.deploy.skip=true -DskipLocalDockerBuild=true -Ddocker.image.prefix=gcr.io/salus-220516 -s .mvn/settings.xml deploy'
                }
            }
        }
    }
}


