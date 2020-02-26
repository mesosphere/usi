pipeline {
  agent none
  stages {
    stage("Build") {
      agent {
        docker {
	  image 'gradle:6.2.1-jdk8'
          label 'large'
        }
      }
      steps {
        ansiColor('xterm') {
          sh '''
            ./gradlew --parallel checkScalaFmtAll && sudo ./gradlew provision && ./gradlew ci --info
	  '''
	}
      }
    }
  }
}
