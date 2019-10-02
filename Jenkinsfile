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
      checkout scm
      sh './gradlew publish --info'
    } 
  }
}
