pipeline {
    agent any

    stages {
		stage('Clean Directory') {
            steps {
                script {
                    sh '''
                        echo "Cleaning workspace directory..."
                        sudo rm -rf ./*
                    '''
                }
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
                    sh '''
                        runningContainers=$(docker ps -q)
                        if [ -n "$runningContainers" ]; then
                            docker stop $runningContainers
                        else
                            echo "No running containers found."
                        fi
                    '''
                }
            }
        }

        stage('Delete Docker Images') {
            steps {
                script {
                    sh '''
                        dockerImages=$(docker images -q)
                        if [ -n "$dockerImages" ]; then
                            docker rmi -f $dockerImages
                        else
                            echo "No Docker images to delete."
                        fi
                    '''
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

        stage('Append .env File') {
            steps {
                sh '''
                    cat /jenkins/.env > ./.env
                '''
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

        stage('Execute Docker Compose Up Airflow Init') {
            steps {
                // Execute docker-compose up airflow-init
                sh 'sudo docker compose up airflow-init'
            }
        }
		
	stage('Execute Docker Compose Up in Detached Mode ') {
            steps {
                // Execute docker-compose up airflow-init
                sh 'sudo docker compose up -d'
            }
        }

	stage('Docker Compose Down') {
            steps {
                // Execute docker-compose down
                sh 'sudo docker compose down'
            }
        }

        stage('Docker Compose Up with Force Recreate') {
            steps {
                // Execute docker-compose up with force recreate in detached mode
                sh 'sudo docker compose up --force-recreate -d'
            }
        }
    }
}
