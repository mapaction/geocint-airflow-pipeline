pipeline {
    agent any

    triggers {
        pollSCM('H/5 * * * *')
    }

    stages {
        stage('Change permissions to Jenkins') {
            steps {
                // Clean workspace using rm -rf command, including hidden files
                sh 'sudo rm -rf ./* .[^.]*'
                // Change ownership recursively to jenkins user and group
                sh 'sudo chown -R jenkins:jenkins * .*'
            }
        }

        stage('Clone Repository') {
            steps {
                // Checkout code from SCM
                git branch: 'staging', url: 'https://github.com/mapaction/geocint-airflow-pipeline.git'
            }
        }

        stage('Stop Docker Containers') {
            steps {
                script {
                    // Check if there are any running containers
                    def runningContainers = sh(
                        script: 'docker ps -q',
                        returnStdout: true
                    ).trim()

                    // If there are running containers, stop them
                    if (runningContainers) {
                        sh "docker stop \$(docker ps -q)"
                    } else {
                        echo "No running containers found."
                    }
                }
            }
        }

        stage('Delete Docker Images') {
            steps {
                script {
                    def imageIds = sh(script: 'docker images -q', returnStdout: true).trim()
                    if (imageIds) {
                        sh "docker rmi -f ${imageIds}"
                    } else {
                        echo "No Docker images found to delete."
                    }
                }
            }
        }

        stage('System Prune') {
            steps {
                sh 'docker system prune -af'
            }
        }

        stage('Create Directories') {
            steps {
                sh 'mkdir -p ./dags ./logs ./plugins ./config'
            }
        }

        stage('Append .env File') {
            steps {
                sh 'cat /jenkins/.env > ./.env'
            }
        }

        stage('Create Directory Downloaded Data') {
            steps {
                sh 'mkdir -p ./dags/static_data/downloaded_data'
            }
        }

        stage('Copy Data Files') {
            steps {
                sh 'cp /jenkins/oceans_and_seas.zip ./dags/static_data/downloaded_data/'
                sh 'cp /jenkins/hydrorivers.zip ./dags/static_data/downloaded_data/'
                sh 'cp -r /jenkins/global_Background_shp ./dags/static_data/downloaded_data/'
                sh 'cp /jenkins/credentials.json ./dags/static_data/'
                sh 'sudo cp /jenkins/hdx_configuration.yaml /root/.hdx_configuration.yaml'
            }
        }

        stage('Execute Docker Compose Up Airflow Init') {
            steps {
                sh 'sudo docker compose up airflow-init'
            }
        }
		
        stage('Execute Docker Compose Up in Detached Mode') {
            steps {
                sh 'sudo docker compose up -d'
            }
        }

        stage('Docker Compose Down') {
            steps {
                sh 'sudo docker compose down'
            }
        }

        stage('Docker Compose Up with Force Recreate') {
            steps {
                sh 'sudo docker compose up --force-recreate -d'
            }
        }
    }
}
