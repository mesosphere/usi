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
        docker {
          image 'hseeberger/scala-sbt:8u252_1.3.10_2.13.2'
          label 'large'
        }
      }
        // TODO: enable later
//      when {
//        branch 'master'
//      }
      steps {
        // mesosphere-ci (mesosphere-ci on Github) mesosphereci-github
        sshagent(credentials: ['4ff09dce-407b-41d3-847a-9e6609dd91b8']) {
          sh 'git config --global user.name "MesosphereCI Robot"'
          sh 'git config --global user.email "mesosphere-ci@users.noreply.github.com"'
          sh 'rm -r ~/.sbt/ghpages/ || true'
          sh 'sbt docs/ghpagesPushSite'
        }
      }
    }
  }
}
