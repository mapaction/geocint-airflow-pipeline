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
                script {
                    def runningContainers = sh(
                        script: 'docker ps -aq',
                        returnStdout: true
                    ).trim()

                    if (runningContainers) {
                        // Stop all running containers
                        sh "docker stop ${runningContainers}"
                    } else {
                        echo "No running containers found."
                    }
                }
            }
        }

        stage('Delete Docker Images') {
            steps {
                script {
                    def dockerImages = sh(
                        script: 'docker images -aq',
                        returnStdout: true
                    ).trim()

                    if (dockerImages) {
                        // Delete all Docker images
                        sh "docker rmi -f ${dockerImages}"
                    } else {
                        echo "No Docker images to delete."
                    }
                }
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
                // Copy data files from /jenkins to ./dags/static_data/downloaded_data/
                sh 'cp /jenkins/oceans_and_seas.zip ./dags/static_data/downloaded_data/'
                sh 'cp /jenkins/hydrorivers.zip ./dags/static_data/downloaded_data/'
                sh 'cp -r /jenkins/global_Background_shp ./dags/static_data/downloaded_data/'
                sh 'cp /jenkins/credentials.json ./dags/static_data/'
            }
        }

    }
}
