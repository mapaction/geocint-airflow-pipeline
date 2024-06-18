pipeline {
    agent any

    stages {
        stage('Clone Repository') {
            steps {
                // Checkout code from SCM 
                git branch: 'master', url: 'https://github.com/mapaction/geocint-airflow-pipeline.git'
            }
        }

        stage('Install Dependencies') {
            steps {
                // Install flake8 using pip
                sh 'pip install flake8'
            }
        }

        stage('Run flake8 Tests') {
            steps {
                // Run flake8 to check Python files recursively
                sh 'flake8 .'
            }
        }
    }
}
