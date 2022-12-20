import numpy as np
import pandas as pd
from os.path import exists
from requests_toolbelt import MultipartEncoder
import xmltodict as xd
import pyparsing
import os, json, requests, re, sys
from datetime import datetime
import pytz
import smtplib
import yagmail

import email, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class CdeConnection:
    '''Class to establish a connection to a CDE Virtual Cluster
       and interact with it e.g. upload Spark CDE Job files'''

    def __init__(self, JOBS_API_URL, WORKLOAD_USER, WORKLOAD_PASSWORD):
        self.JOBS_API_URL = JOBS_API_URL
        self.WORKLOAD_USER = WORKLOAD_USER
        self.WORKLOAD_PASSWORD = WORKLOAD_PASSWORD
        #self.GMAIL_APP_PASSWORD = GMAIL_APP_PASSWORD

    # Set user token to interact with CDE Service remotely
    def set_cde_token(self):

        rep = self.JOBS_API_URL.split("/")[2].split(".")[0]
        os.environ["GET_TOKEN_URL"] = self.JOBS_API_URL.replace(rep, "service").replace("dex/api/v1", "gateway/authtkn/knoxtoken/api/v1/token")

        token_json = requests.get(os.environ["GET_TOKEN_URL"], auth=(self.WORKLOAD_USER, self.WORKLOAD_PASSWORD))

        return json.loads(token_json.text)["access_token"]

    # Create CDE Resource to upload Spark CDE Job files
    def create_cde_resource(self, token, resource_name):

        print("Started Creating Resource {}".format(resource_name))

        url = self.JOBS_API_URL + "/resources"
        myobj = {"name": str(resource_name)}
        data_to_send = json.dumps(myobj).encode("utf-8")

        headers = {
            'Authorization': f"Bearer {token}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        x = requests.post(url, data=data_to_send, headers=headers)

        if x.status_code == 201:
            print("Creating Resource {} has Succeeded".format(resource_name))
        else:
            print(x.status_code)
            print(x.text)

    #Upload Spark CDE Job file to CDE Resource
    def upload_file(self, resource_name, job_path, file_name, token):

        print("Uploading File {0} to CDE Resource {1}".format(file_name, resource_name))

        m = MultipartEncoder(
            fields={
                    'file': ('filename', open(job_path+"/"+file_name, 'rb'), 'text/plain')}
            )

        PUT = '{jobs_api_url}/resources/{resource_name}/{file_name}'.format(jobs_api_url=os.environ["JOBS_API_URL"], resource_name=resource_name, file_name=file_name)

        x = requests.put(PUT, data=m, headers={'Authorization': f"Bearer {token}",'Content-Type': m.content_type})
        print("Response Status Code {}".format(x.status_code))

        if x.status_code == 201:
            print("Uploading File {0} to CDE Resource {1} has Succeeded".format(file_name, resource_name))
        else:
            print(x.status_code)
            print(x.text)

    def create_spark_job_from_resource(self, token, cde_job_name, CDE_RESOURCE_NAME, PYSPARK_EXAMPE_SCRIPT_NAME, spark_confs={"spark.pyspark.python": "python3"}):

        print("Started Creating CDE Spark Job {0} with Script {1}".format(cde_job_name, PYSPARK_EXAMPE_SCRIPT_NAME))

        ### Any Spark Job Configuration Options (Not Mandatory) ###
        #spark_confs_example = {
                  #"spark.dynamicAllocation.maxExecutors": "6",
                  #"spark.dynamicAllocation.minExecutors": "2",
                  #"spark.executor.extraJavaOptions": "-Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true",
                  #"spark.hadoop.fs.s3a.metadatastore.impl": "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore",
                  #"spark.kubernetes.memoryOverheadFactor": "0.2",
                  #"spark.pyspark.python": "python3"
                  #"spark.rpc.askTimeout": "600",
                  #"spark.sql.shuffle.partitions": "48",
                  #"spark.yarn.access.hadoopFileSystems": "s3a://your_data_lake_here"
                #}

        cde_payload = {
              "name": cde_job_name,# CDE Job Name As you want it to appear in the CDE JOBS UI
              "type": "spark",
              "retentionPolicy": "keep_indefinitely",
              "mounts": [
                {
                  "resourceName": CDE_RESOURCE_NAME
                }
              ],
              "spark": {
                "file": PYSPARK_EXAMPE_SCRIPT_NAME,
                "driverMemory": "1g",
                "driverCores": 1, #this must be an integer
                "executorMemory": "4g",
                "executorCores": 1, #this must be an integer
                "conf": spark_confs,
                "logLevel": "INFO"
              },
              "schedule": {
                "enabled": False,
                "user": self.WORKLOAD_USER #Your CDP Workload User is automatically set by CML as an Environment Variable
              }
            }

        headers = {
            'Authorization': f"Bearer {token}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        PUT = '{}/jobs'.format(self.JOBS_API_URL)

        data = json.dumps(cde_payload)

        x = requests.post(PUT, headers=headers, data=data)

        if x.status_code == 201:
            print("Creating CDE Spark Job {0} with Script {1} has Succeeded".format(cde_job_name, PYSPARK_EXAMPE_SCRIPT_NAME))
        else:
            print(x.status_code)
            print(x.text)

    def run_spark_job(self, token, cde_job_name, driver_cores = 2, driver_memory = "4g", executor_cores = 4, executor_memory = "4g", num_executors = 4):

        print("Started to Submit Spark Job {}".format(cde_job_name))

        cde_payload = {"overrides":
                       {"spark":
                        {"driverCores": driver_cores,
                         "driverMemory": driver_memory,
                         "executorCores": executor_cores,
                         "executorMemory": executor_memory,
                         "numExecutors": num_executors}
                       }
                      }

        headers = {
            'Authorization': f"Bearer {token}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        POST = "{}/jobs/".format(self.JOBS_API_URL)+cde_job_name+"/run"

        data = json.dumps(cde_payload)

        x = requests.post(POST, headers=headers, data=data)

        if x.status_code == 201:
            print("Submitting CDE Spark Job {} has Succeeded".format(cde_job_name))
            print("This doesn't necessarily mean that the CDE Spark Job has Succeeded")
            print("Please visit the CDE Job Runs UI to check on CDE Job Status")
        else:
            print(x.status_code)
            print(x.text)

    def list_cde_job_runs(self, token):
        tz_LA = pytz.timezone('America/Los_Angeles')
        now = datetime.now(tz_LA)
        print("Listing Jobs as of: {} PACIFIC STANDARD TIME".format(now))

        url = self.JOBS_API_URL + "/job-runs"

        headers = {
            'Authorization': f"Bearer {token}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        x = requests.get(url+'?limit=100&offset=0&orderby=ID&orderasc=false', headers=headers)

        return x

        if x.status_code == 201:
            print("Listing Jobs {} has Succeeded".format(resource_name))
        else:
            print(x.status_code)
            print(x.text)

    def print_vc_meta(self, token):

        headers = {
            'Authorization': f"Bearer {token}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
            }

        x = requests.get(self.JOBS_API_URL+'/info', headers=headers)
        cde_vc_name = json.loads(x.text)["appName"]
        #cde_vc_id = json.loads(x.text)["appId"]
        #cde_vc_console_url = json.loads(x.text)["cdeConsoleURL"]
        #cde_cluster_id = json.loads(x.text)["clusterID"]
        #cde_version = json.loads(x.text)["version"]

        return cde_vc_name

    def detect_laggers(self, response, MAX_JOB_DURATION_SECONDS=1800):
        #Compare Start with End Dates for Current Job Runs
        tz_LA = pytz.timezone('America/Los_Angeles')
        now = datetime.now(tz_LA)

        df = pd.DataFrame(response.json()['runs'])
        df['started'] = pd.to_datetime(df['started'], infer_datetime_format=True)
        df["timedelta"] = df['started'] - pd.Timestamp(now).to_datetime64()
        laggers_df = df[-df["timedelta"].dt.total_seconds() > MAX_JOB_DURATION_SECONDS]
        laggers_df = laggers_df[(laggers_df["status"] == "running") | (laggers_df["status"] == "starting")] | (laggers_df["status"] == "preparing")]
        laggers_df = laggers_df.reset_index()

        return laggers_df, MAX_JOB_DURATION_SECONDS

    def print_runs(self, response):
        #Compare Start with End Dates for Current Job Runs
        tz_LA = pytz.timezone('America/Los_Angeles')
        now = datetime.now(tz_LA)

        df = pd.DataFrame(response.json()['runs'])
        df['started'] = pd.to_datetime(df['started'],infer_datetime_format=True)
        df["timedelta"] = df['started'] - pd.Timestamp(now).to_datetime64()

        return df

    def smtplib_email_alert(self, laggers_df, job_duration_seconds, sender, receiver, SMTP, cde_vc_name):

        minutes = str(float(job_duration_seconds)/60)

        subject = "CDE Alerter Warning - Potential Issue Found in {}".format(cde_vc_name)
        body = ""

        for i in range(len(laggers_df)):
            body += """

            Run ID {0} of CDE Job {1} owned by User {2} has run for more than {4} minutes and is now in {3} Status.

            """.format(laggers_df['id'][i], laggers_df['job'][i], laggers_df['user'][i], laggers_df['status'][i], minutes)

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender
        message["To"] = receiver
        message["Subject"] = subject

        # Add body to email
        message.attach(MIMEText(body, "plain"))

        try:
           smtpObj = smtplib.SMTP(SMTP)
           smtpObj.sendmail(sender, receiver, message.as_string())
           print("Successfully sent email")

        except smtplib.SMTPException:
           print("Error: unable to send email")

    #def send_email_alert(self, laggers_df, job_duration_seconds, *destination_emails):
        #Send email alerts to destination emails
        #Destination emails is a single or multiple strings
        #yag = yagmail.SMTP('cdemachine10', self.GMAIL_APP_PASSWORD)
        #subject = 'URGENT: Potential CDE Virtual Cluster Issue Detected'

        #body = ''
        #minutes = str(float(job_duration_seconds)/60)

        #for i in range(len(laggers_df)):
            #body += '\n'
            #body += 'Job {0} owned by User {1} has been in {2} Status for more than {3} minutes'\
                #.format(laggers_df['job'][i], laggers_df['user'][i], laggers_df['status'][i], minutes)

        #yag.send(to = destination_emails[0], subject = subject, contents = body)

#    def smtplib_email_alert(self, laggers_df, job_duration_seconds, sender, receiver, SMTP, cde_vc_name):
#
#        minutes = str(float(job_duration_seconds)/60)
#        message = """From: {0}
#
#        To: {1}
#
#        Subject: URGENT CDE Virtual Cluster Alert!
#
#        This is an alert related to CDE Virtual Cluster {2}
#
#        """.format(sender, receiver, cde_vc_name)
#
#        for i in range(len(laggers_df)):
#            message += """
#
#            Run ID {0} of CDE Job {1} owned by User {2} has run for more than {4} minutes and is now in {3} Status.
#
#            """.format(laggers_df['id'][i], laggers_df['job'][i], laggers_df['user'][i], laggers_df['status'][i], minutes)
#
#        try:
#           smtpObj = smtplib.SMTP(SMTP)
#           smtpObj.sendmail(sender, receiver, message)
#           print("Successfully sent email")
#
#        except smtplib.SMTPException:
#           print("Error: unable to send email")
