#!/usr/bin/env groovy

@Library('sec_ci_libs@v2-latest') _

def master_branches = ["master", ] as String[]

ansiColor('xterm') {
  // using shakedown node because it's a lightweight alpine docker image instead of full VM
  node('shakedown') {
    stage("Verify author") {
      user_is_authorized(master_branches, '8b793652-f26a-422f-a9ba-0d1e47eb9d89', '#eng-jenkins-builds')
    }
  }
  node('jdk8-scala') {
    stage('Publish') {
      withCredentials([
        string(credentialsId: '3f0dbb48-de33-431f-b91c-2366d2f0e1cf',variable: 'AWS_ACCESS_KEY_ID'),
        string(credentialsId: 'f585ec9a-3c38-4f67-8bdb-79e5d4761937',variable: 'AWS_SECRET_ACCESS_KEY'),
      ]) {
      sshagent (credentials: ['0f7ec9c9-99b2-4797-9ed5-625572d5931d']) {
        checkout scm
        sh './gradlew --parallel publish --info'
      }
    }}
  }
}
