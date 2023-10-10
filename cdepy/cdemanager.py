"""
Module to manage CDE Clusters
"""

from cdepy.cdeconnection import CdeConnection
import requests
from datetime import datetime
import pytz
import numpy as np
import pandas as pd
from os.path import exists
from requests_toolbelt import MultipartEncoder
import xmltodict as xd
import pyparsing
import os, json, requests, re, sys


class CdeClusterManager:
    """
    Class to manage CDE Clusters
    """

    def __init__(self, cdeConnection):
        self.clusterConnection = cdeConnection
        self.JOBS_API_URL = self.clusterConnection.JOBS_API_URL
        self.TOKEN = self.clusterConnection.TOKEN

    def createJob(self, cdeJobDefinition):
        """
        Method to create a CDE Job
        Requires cdeJobDefinition of type cdeAirflowJobDefinition or cdeSparkJobDefinition as input for payload
        """

        headers = {
            'Authorization': f"Bearer {self.TOKEN}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        PUT = '{}/jobs'.format(self.JOBS_API_URL)

        data = json.dumps(cdeJobDefinition)

        x = requests.post(PUT, headers=headers, data=data)

        if x.status_code == 201:
            print("CDE Job Creation Succeeded\n")
        else:
            print(x.status_code)
            print(x.text)


    def deleteJob(self):
        raise NotImplementedError

    def listJobs(self):
        raise NotImplementedError

    def listJobRuns(self):
        """
        Method to show all CDE Jobs that have been executed in the cluster
        Does not require input
        """

        tz_LA = pytz.timezone('America/Los_Angeles')
        now = datetime.now(tz_LA)
        print("Listing Jobs as of: {} PACIFIC STANDARD TIME\n".format(now))

        url = self.JOBS_API_URL + "/job-runs"

        headers = {
            'Authorization': f"Bearer {self.TOKEN}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        x = requests.get(url+'?limit=100&offset=0&orderby=ID&orderasc=false', headers=headers)

        return x.text

        if x.status_code == 201:
            print("Listing Jobs Succeeded")
        else:
            print(x.status_code)
            print(x.text)

    def runJob(self, CDE_JOB_NAME):
        """
        Method to trigger execution of CDE Job
        CDE Job could be of type Spark or Airflow
        The method assumes the CDE Job has already been created
        """

        payloadData = {"hidden":False}

        headers = {
            'Authorization': f"Bearer {self.TOKEN}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        POST = "{}/jobs/".format(self.JOBS_API_URL)+CDE_JOB_NAME+"/run"

        data = json.dumps(payloadData)

        x = requests.post(POST, headers=headers, data=data)

        if x.status_code == 201:
            print("CDE Job Submission has Succeeded\n")
            print("Please visit the CDE Job Runs UI to validate CDE Job Status\n")
        else:
            print(x.status_code)
            print(x.text)

    def createResource(self, cdeRsourceDefinition):
        """
        Method to create CDE Resource
        Requires cdeRsourceDefinition as input
        Accepts types Files or Python
        e.g. cdeRsourceDefinition = {"name": str(resource_name)}
        """

        print("CDE Resource Creation in Progress\n")

        url = self.JOBS_API_URL + "/resources"
        data_to_send = json.dumps(cdeRsourceDefinition).encode("utf-8")

        headers = {
            'Authorization': f"Bearer {self.TOKEN}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        x = requests.post(url, data=data_to_send, headers=headers)

        if x.status_code == 201:
            print("CDE Resource Created Successfully")
        else:
            print(x.status_code)
            print(x.text)

    def deleteResource(self, cdeResourceName):
        """
        Method to delete CDE Resource
        Requires cdeRsourceDefinition name as input
        e.g. cdeResourceName = str(resource_name)
        """

        print("CDE Resource Deletion in Progress\n")

        headers = {
            'accept': 'application/json',
        }

        url = self.JOBS_API_URL + "/resources/" + cdeResourceName

        response = requests.delete(url, headers=headers)

        if x.status_code == 201:
            print("CDE Resource Deleted Successfully\n")
        else:
            print(x.status_code)
            print(x.text)

    #Upload Spark CDE Job file to CDE Resource
    def uploadFile(self, CDE_RESOURCE_NAME, LOCAL_FILE_PATH, LOCAL_FILE_NAME):
        """
        Method to uplaod files from local to CDE Resource
        Can be used to:
            1) upload files to a CDE Resource of type Files
            2) uplaod a "requirements.txt" file to a CDE Resource of type Python Environment
        Requires a CDE_RESOURCE_NAME, LOCAL_FILE_PATH and LOCAL_FILE_NAME
        e.g. "myCdeFilesResource", "~/myfiles/cdefiles", and "mySparkScript.py"
        e.g. "myCdePythonResource", "~/myfiles/cdefiles", and "requirements.txt"
        """

        print("Uploading File {0} to CDE Resource {1}\n".format(LOCAL_FILE_NAME, CDE_RESOURCE_NAME))

        m = MultipartEncoder(
            fields={
                    'file': ('filename', open(LOCAL_FILE_PATH+"/"+LOCAL_FILE_NAME, 'rb'), 'text/plain')}
            )

        PUT = '{jobs_api_url}/resources/{resource_name}/{file_name}'.format(jobs_api_url=self.JOBS_API_URL, resource_name=CDE_RESOURCE_NAME, file_name=LOCAL_FILE_NAME)

        x = requests.put(PUT, data=m, headers={'Authorization': f"Bearer {self.TOKEN}",'Content-Type': m.content_type})
        print("Response Status Code {}".format(x.status_code))

        if x.status_code == 201:
            print("Uploading File {0} to CDE Resource {1} has Succeeded\n".format(LOCAL_FILE_NAME, CDE_RESOURCE_NAME))
        else:
            print(x.status_code)
            print(x.text)
