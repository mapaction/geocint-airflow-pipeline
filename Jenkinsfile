pipeline {
    agent any

    triggers {
        pollSCM('H/5 * * * *')
    }

    stages {
        stage('Clean Directory') {
            steps {
                script {
                    // Lock to ensure sequential execution
                    lock('clean-directory-lock') {
                        sh '''
                            echo "Cleaning workspace directory..."
                            sudo rm -rf ./*
                        '''
                    }
                }
            }
        }

        stage('Clone Repository') {
            steps {
                // Lock to ensure sequential execution
                lock('clean-directory-lock') {
                    // Checkout code from SCM
                    git branch: 'master', url: 'https://github.com/mapaction/geocint-airflow-pipeline.git'
                }
            }
        }

        stage('Stop Docker Containers') {
            steps {
                script {
                    // No need to lock here unless required for specific reasons
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
                    // No need to lock here unless required for specific reasons
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
                // No need to lock here unless required for specific reasons
                sh 'docker system prune -af'
            }
        }

        stage('Create Directories') {
            steps {
                // No need to lock here unless required for specific reasons
                sh 'mkdir -p ./dags ./logs ./plugins ./config'
            }
        }

        stage('Append .env File') {
            steps {
                // No need to lock here unless required for specific reasons
                sh '''
                    cat /jenkins/.env > ./.env
                '''
            }
        }

        stage('Create Directory Downloaded Data') {
            steps {
                // No need to lock here unless required for specific reasons
                sh 'mkdir -p ./dags/static_data/downloaded_data'
            }
        }

        stage('Copy Data Files') {
            steps {
                // No need to lock here unless required for specific reasons
                sh 'cp /jenkins/oceans_and_seas.zip ./dags/static_data/downloaded_data/'
                sh 'cp /jenkins/hydrorivers.zip ./dags/static_data/downloaded_data/'
                sh 'cp -r /jenkins/global_Background_shp ./dags/static_data/downloaded_data/'
                sh 'cp /jenkins/credentials.json ./dags/static_data/'
            }
        }

        stage('Execute Docker Compose Up Airflow Init') {
            steps {
                // No need to lock here unless required for specific reasons
                sh 'sudo docker compose up airflow-init'
            }
        }

        stage('Execute Docker Compose Up in Detached Mode') {
            steps {
                // No need to lock here unless required for specific reasons
                sh 'sudo docker compose up -d'
            }
        }

        stage('Docker Compose Down') {
            steps {
                // No need to lock here unless required for specific reasons
                sh 'sudo docker compose down'
            }
        }

        stage('Docker Compose Up with Force Recreate') {
            steps {
                // No need to lock here unless required for specific reasons
                sh 'sudo docker compose up --force-recreate -d'
            }
        }
    }

    post {
        always {
            // Release the lock after the pipeline completes
            lock('clean-directory-lock', true)
        }
    }
}
