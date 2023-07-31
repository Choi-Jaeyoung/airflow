
pipeline {
    agent any
    environment {
        JENKINS_BASE_URL="http://localhost:19800"
        SLACK_CHANNEL="dataplatform-channel"
        BUILD_ENV="DEV"
    }
    
    stages {
        stage('Copy DAG') {
            steps {
                script {
                    try {
                        
                        echo "---- Copy DAG ----"

                        // sendInfoNotification("================================\nBuilding ($BUILD_ENV) 소스 반영을 시작합니다...\n$JENKINS_BASE_URL/job/" +  env.JOB_NAME + '/' + env.BUILD_NUMBER + '/console')
                        
                        sh "rm -rf /var/dags/*"
                        sh "cp -r /var/jenkins_home/workspace/airflow-deploy/* /var/dags/"

                        // sendSuccessNotification()

                    } catch (error) {
                        print(error)
                        // sendErrorNotification("Git clone  실패했습니다! 확인하세요.")
                        
                        env.cloneResult=false
                        currentBuild.result = 'FAILURE'
                    }
                }
            }
        }

        
    }
}



def sendInfoNotification(msg) {
    slackSend(channel: "$SLACK_CHANNEL"
        , message: msg
        , color: '#0400ff', tokenCredentialId: 'slack-key')
}

def sendSuccessNotification() {
    
    def msg = "Deploying ($BUILD_ENV) 성공했습니다.~" + "\n================================"
    
    slackSend(channel: "$SLACK_CHANNEL"
        , message: msg
        , color: '#fff700', tokenCredentialId: 'slack-key')
    
}

def sendErrorNotification(msg) {
    
    print msg
    
    def errMsg = "BUILD FAILURE - ($BUILD_ENV) 실패했습니다. 어서 확인하세요." +
        "\n${msg}" +
        "\nPlease refer the build result on the console.\n$JENKINS_BASE_URL/job/" +  env.JOB_NAME + '/' + env.BUILD_NUMBER + '/console' + '\n================================'
    
    slackSend(channel: "$SLACK_CHANNEL"
        , message: errMsg
        , color: '#e04343', tokenCredentialId: 'slack-key')
}

