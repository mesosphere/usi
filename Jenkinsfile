#!/usr/bin/env groovy

@Library('sec_ci_libs@v2-latest') _

def master_branches = ["master", ] as String[]

pipeline {
  agent none

  stages {
    stage("Verify author for PR") {
      // using shakedown node because it's a lightweight Alpine Docker image instead of full VM
      agent {
        label "shakedown"
      }
      when {
        beforeAgent true
        changeRequest()
      }
      steps {
        user_is_authorized(master_branches, '8b793652-f26a-422f-a9ba-0d1e47eb9d89', '#dcos-security-ci')
      }
    }

    stage("Publish Packages") {
      agent {
        docker {
          image 'mesosphere/scala-sbt:marathon'
          label 'large'
        }
      }

      environment {
        AWS_ACCESS_KEY_ID = credentials('3f0dbb48-de33-431f-b91c-2366d2f0e1cf')
        AWS_SECRET_ACCESS_KEY = credentials('f585ec9a-3c38-4f67-8bdb-79e5d4761937')
      }

      steps {
        sh 'echo skip'
    //    sh 'sbt +publish'
      }
    }
    stage('Publish Documentation') {
      agent {
        label 'JenkinsMarathonCI-Debian9-2020-01-14'
      }
        // TODO: enable later
//      when {
//        branch 'master'
//      }
      steps {
        sshagent(credentials: ['mesosphereci-github']) {
          sh 'sudo chown -R admin /home/admin/.cache'
          //sh 'git config --global user.name "MesosphereCI Robot"'
          //sh 'git config --global user.email "mesosphere-ci@users.noreply.github.com"'
          //sh 'git config core.sshCommand "ssh -v -o StrictHostKeyChecking=no"'
          sh 'rm -r ~/.sbt/ghpages/ || true'
          sh 'sbt docs/ghpagesPushSite'
        }
      }
    }
  }
}
