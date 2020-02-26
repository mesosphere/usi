pipeline {
  agent none
  stages {
    stage("Build") {
      agent {
        docker {
	  image 'gradle:5.1.1-jdk8'
          label 'large'
        }
      }
      steps {
        ansiColor('xterm') {
          sh '''
            ./gradlew --parallel checkScalaFmtAll && ./gradlew assemble --info
	  '''
	}
      }
    }
  }
}
