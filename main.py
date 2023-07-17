import subprocess

import paramiko
from google.oauth2 import service_account
import google.auth
from google.auth.transport import requests
import requests as http_requests
import time
from flask import Flask, request
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

scheduler = BackgroundScheduler()

service_account_file = '/path/to/service-account-key.json'
project_id = 'your-project-id'


def get_oidc_token():
    credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    auth_request = google.auth.transport.requests.Request()

    credentials.refresh(auth_request)
    token = credentials.token

    return token


def get_ssh_keys(user_email):
    token = get_oidc_token()
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    response = http_requests.get(
        f'https://oslogin.googleapis.com/v1/users/{user_email}/sshPublicKeys',
        headers=headers
    )

    ssh_keys = response.json()
    return ssh_keys


def execute_query(execution_time, query, host, schema, table, mysql_root_password, ssh_username, ssh_keys):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=ssh_username, pkey=ssh_keys)

    # Execute the percona query in the mysql server

    percona_query = f"(pt-online-schema-change --alter '{query}' D='{schema}',t='{table}',h=localhost,p='{mysql_root_password}',u=root --version-check --swap-tables --preserve-triggers  --statistics --progress=percentage,5 --no-drop-old-table --noanalyze-before-swap --max-lag 300 --check-interval 1 --chunk-size 1000 --alter-foreign-keys-method=drop_swap --recursion-method=none --execute > schema-change-output 2>&1) &"
    execution_command = f"at '{execution_time}' -f {percona_query}'"
    subprocess.call(execution_command, shell=True)
#    stdin, stdout, stderr = ssh.exec_command(percona_query)

    ssh.close()


# Error handling


@app.route('/schedule_query', methods=['POST'])
def schedule_query():
    query = request.form['query']
    execution_time = request.form['executionTime']
    host = request.form['host']
    schema = request.form['schema']
    table = request.form['table']

    # perform validation and error handling

    # scheduling the query in the background using apscheduler


    return 'Query scheduled successfully'


if __name__ == '__main__':
    app.run()
