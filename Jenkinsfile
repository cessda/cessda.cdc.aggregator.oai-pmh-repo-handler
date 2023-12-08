#!/usr/bin/env groovy

node_label = 'jnlp-himem'
email_recipient = ''

def sendmail(build_result) {
    // TODO: recipients should be dynamically resolved from changeset authors
    // and passed here as parameters.
    // recipients is string of comma separated email addresses.
    stage('Send mail') {
        mail(to: email_recipient,
             subject: "Job '${env.JOB_NAME}' (${env.BUILD_NUMBER}) result is ${build_result}",
             body: "See info at ${env.BUILD_URL}")
    }
}

// Discard old builds
properties([buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '', numToKeepStr: '30'))])

// node reserves executor and workspace
node(node_label) {
    // Prepare
    // -------
    def toxEnvName = '.env-tox'
    def pylintEnvName = '.env-pylint'
    def sqScannerHome = tool 'sonar-scanner'
    def pylint_report_path = 'pylint_report.txt'
    def coverage_xml_path = 'coverage.xml'
    def sonar_properties_path = 'sonar-project.properties'
    def image_tag = "${DOCKER_ARTIFACT_REGISTRY}/cdc-agg-oai:${env.BRANCH_NAME.toLowerCase().replaceAll('[^a-z0-9\\.\\_\\-]', '-')}-${env.BUILD_NUMBER}"

    // prepare workspace
    def myworkspace = ''

    def tasks_1 = [:]
    def tasks_2 = [:]
    def tasks_3 = [:]

    myworkspace = "${WORKSPACE}"
    echo "My workspace is ${myworkspace}"
    deleteDir()

    // Get recipient from revision author
    checkout scm
    email_recipient = sh (
        script: "git show -s --pretty=%ae",
        returnStdout: true
    )
    echo "Build result will be sent to ${email_recipient}"

    // Assign parallel tasks
    tasks_1['Prepare Tox, Run With Coverage & Publish Report'] = {
        docker.image('python:3.9').inside('-u root') {
            stage('Prepare Tox Venv') {
                if (!fileExists(toxEnvName)) {
                    echo 'Build Python Virtualenv for testing...'
                    sh """
                    python -m venv ${toxEnvName}
                    . ./${toxEnvName}/bin/activate
                    pip install --upgrade pip
                    pip install tox
                    """
                }
            }
            stage('Run Test Suite & Gather Coverage') {
                sh """
                . ./${toxEnvName}/bin/activate
                tox -e with-coverage
                """
            }
            stage('Publish Cobertura Report') {
                cobertura(coberturaReportFile: "${coverage_xml_path}",
                          zoomCoverageChart: false)
            }
            stage('Clean up tox-env') {
                if (fileExists(toxEnvName)) {
                    sh "rm -r ${toxEnvName}"
                }
            }
        }
    }
    tasks_1['Prepare Pylint, Run Analysis, Archive & Publish report'] = {
        docker.image('python:3.9').inside('-u root') {
            stage('Prepare Pylint Venv') {
                if (!fileExists(pylintEnvName)) {
                    sh """
                    python -m venv ${pylintEnvName}
                    . ./${pylintEnvName}/bin/activate
                    pip install --upgrade pip
                    pip install -r ./requirements.txt
                    pip install .
                    pip install pylint
                    """
                }
            }
            stage('Run PyLint') {
                echo 'Run pylint'
                sh """
                . ./${pylintEnvName}/bin/activate
                pylint -f parseable cdcagg_oai | tee ${pylint_report_path}
                """
            }
            stage('Archive PyLint Report') {
                archiveArtifacts artifacts: pylint_report_path
            }
            stage('Publish PyLint Report') {
                recordIssues tool: pyLint(pattern: pylint_report_path)
            }
        }
    }
    tasks_2['Run Tests py38'] = {
        docker.image('python:3.8').inside('-u root') {
            stage('Prepare Tox Venv') {
                if (!fileExists(toxEnvName)) {
                    echo 'Build Python Virtualenv for testing...'
                    sh """
                    python -m venv ${toxEnvName}
                    . ./${toxEnvName}/bin/activate
                    pip install --upgrade pip
                    pip install tox
                    """
                }
            }
            stage('Run Tests') {
                sh """
                . ./${toxEnvName}/bin/activate
                tox -e py38
                """
            }
            stage('Clean up tox-env') {
                if (fileExists(toxEnvName)) {
                    sh "rm -r ${toxEnvName}"
                }
            }
        }
    }
    tasks_2['Publish Reports & Initiate SonarQube Analysis'] = {
        stage('Prepare sonar-project.properties') {
            sh "echo sonar.projectVersion = \$(cat VERSION) >> ${sonar_properties_path}"
        }
        stage('Initiate SonarQube analysis') {
            withSonarQubeEnv() {
                sh "${sqScannerHome}/bin/sonar-scanner"
            }
        }
    }
    tasks_3['Run Tests py310'] = {
        docker.image('python:3.10').inside('-u root') {
            stage('Prepare Tox Venv') {
                if (!fileExists(toxEnvName)) {
                    echo 'Build Python Virtualenv for testing...'
                    sh """
                    python -m venv ${toxEnvName}
                    . ./${toxEnvName}/bin/activate
                    pip install --upgrade pip
                    pip install tox
                    """
                }
            }
            stage('Run Tests') {
                sh """
                . ./${toxEnvName}/bin/activate
                tox -e py310
                """
            }
            stage('Clean up tox-env') {
                if (fileExists(toxEnvName)) {
                    sh "rm -r ${toxEnvName}"
                }
            }
        }
    }
    try {
        // run parallel tasks
        parallel tasks_1
        parallel tasks_2
        parallel tasks_3
        stage('Build docker image') {
            withEnv(['DOCKER_BUILDKIT=1']) {
                sh "docker build -t ${image_tag} --secret id=bbcreds,env=BBCREDS ."
            }
        }
        if (env.BRANCH_NAME == 'main') {
            stage('Push Docker image') {
                sh "gcloud auth configure-docker ${ARTIFACT_REGISTRY_HOST}"
                sh "docker push ${image_tag}"
                sh "gcloud artifacts docker tags add ${image_tag} ${DOCKER_ARTIFACT_REGISTRY}/cdc-agg-oai:${env.BRANCH_NAME}-latest"
            }
        }
    } catch (err) {
        currentBuild.result = 'FAILURE'
        sendmail('FAILURE')
    }
}
// Wait for sonar quality gate
stage("Quality Gate") {
    timeout(time: 10, unit: 'MINUTES') { // Just in case something goes wrong, pipeline will be killed after a timeout
        def qg = waitForQualityGate() // Reuse taskId previously collected by withSonarQubeEnv
        if (qg.status != 'OK') {
            echo "Pipeline unstable due to quality gate failure: ${qg.status}"
            currentBuild.result = 'UNSTABLE'
            sendmail('UNSTABLE')
        }
    }
}
