pipeline {
    agent any

    triggers {
        pollSCM('H/5 * * * *')
    }

    stages {
	stage('Change permissions to Jenkins') {
            steps {
               
                sh 'chown -R jenkins:jenkins .'
            }
        }
        stage('Clean Directory') {
            steps {
                // Clean workspace using rm -rf command, including hidden files
                cleanWs()
            }
        }

        stage('Clone Repository') {
            steps {
                // Checkout code from SCM
                git branch: 'master', url: 'https://github.com/mapaction/geocint-airflow-pipeline.git'
            }
        }

        stage('Stop Docker Containers') {
            steps {
                script {
                    runningContainers = sh(
                        script: 'docker ps -q',
                        returnStdout: true
                    ).trim()

                    if (runningContainers) {
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
                    dockerImages = sh(
                        script: 'docker images -q',
                        returnStdout: true
                    ).trim()

                    if (dockerImages) {
                        sh "docker rmi -f ${dockerImages}"
                    } else {
                        echo "No Docker images to delete."
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
