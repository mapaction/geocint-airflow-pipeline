pipeline {
    agent any

    stages {
        stage('Clone Repository') {
            steps {
                // Checkout code from SCM 
                git branch: 'master', url: 'https://github.com/mapaction/geocint-airflow-pipeline.git'
            }
        }

        stage('Stop Docker Containers') {
            steps {
                // Stop all running containers
                sh 'docker stop $(docker ps -aq)'
            }
        }

        stage('Delete Docker Images') {
            steps {
                // Delete all Docker images
                sh 'docker rmi -f $(docker images -aq)'
            }
        }

        stage('System Prune') {
            steps {
                // Prune Docker system (clean up unused data)
                sh 'docker system prune -af'
            }
        }

        // Other stages can be added here as needed

    }
}
