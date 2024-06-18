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

        stage('Create Directories') {
            steps {
                // Create necessary directories
                sh 'mkdir -p ./dags ./logs ./plugins ./config'
            }
        }
		
		stage('Copy .env File') {
            steps {
                // Copy .env file from /jenkins to ./ (Jenkins workspace)
                sh 'cp /jenkins/.env ./.env'
            }
        }
		
		stage('Create Directory Downloaded Data') {
            steps {
                // Create necessary directories if they don't exist
                sh 'mkdir -p ./dags/static_data/downloaded_data'
            }
        }
		
		stage('Copy Data Files') {
            steps {
                // Copy .env file from /jenkins to ./ (Jenkins workspace)
                sh 'cp /jenkins/oceans_and_seas.zip ./dags/static_data/downloaded_data/'
		sh 'cp /jenkins/hydrorivers.zip ./dags/static_data/downloaded_data/'
		sh 'cp -r /jenkins/global_Background_shp ./dags/static_data/downloaded_data/'
            }
        }
		
		
		

    }
}
