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
        label 'jdk8-scala'
      }

      environment {
        AWS_ACCESS_KEY_ID = credentials('3f0dbb48-de33-431f-b91c-2366d2f0e1cf')
        AWS_SECRET_KEY = credentials('f585ec9a-3c38-4f67-8bdb-79e5d4761937')
      }

      steps {
        sh 'mkdir -p /root/.sbt/launchers/1.3.13/'
        sh 'curl -L -o /root/.sbt/launchers/1.3.13/sbt-launch.jar https://repo.scala-sbt.org/scalasbt/maven-releases/org/scala-sbt/sbt-launch/1.3.13/sbt-launch.jar'
        sh 'sbt +publish'
      }
    }

    stage('Publish Documentation') {
      when {
        beforeAgent true
        branch 'master'
      }
      agent {
        label 'JenkinsMarathonCI-Debian9-2020-01-14'
      }
      steps {
        sshagent(credentials: ['mesosphereci-github']) {
	  // /home/admin/.cache was populated with sudo
          sh 'sudo chown -R admin /home/admin/.cache'
	  
	  // Clear cache.
          sh 'rm -r ~/.sbt/ghpages/ || true'

          // Set git config to CI bot.
          sh 'git config --global user.name "MesosphereCI Robot"'
          sh 'git config --global user.email "mesosphere-ci@users.noreply.github.com"'

          sh 'sbt docs/ghpagesPushSite'
        }
      }
    }
  }
}
