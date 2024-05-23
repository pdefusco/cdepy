"""
Module to create a Connection to a CDE Virtual Cluster
"""

import numpy as np
import pandas as pd
from os.path import exists
from requests_toolbelt import MultipartEncoder
import xmltodict as xd
import pyparsing
import os, json, requests, re, sys
from cdepy.cdeconnection import CdeConnection

class CdeAirflowPythonEnv():
  """
  Class to manage Airflow Python Env maintenance sessions
  """

  def __init__(self, cdeConnection):
      self.clusterConnection = cdeConnection
      self.JOBS_API_URL = self.clusterConnection.JOBS_API_URL
      self.TOKEN = self.clusterConnection.TOKEN


  def createMaintenanceSession(self):
    """
    Method to create a Maintenance Session to Manage CDE Airflow Python Environments
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    POST = "{}/admin/airflow/env/maintenance".format(self.JOBS_API_URL)

    x = requests.post(POST, headers=headers)

    if x.status_code == 201:
        print("CDE Maintenance Session Creation has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def createRepositoryPayload(self):
    """
    Method to create the Payload for the createRepository method
    """
    raise NotImplementedError("Can't use thie method yet!")


  def createPipRepository(self):
    """
    Method to create a pip repository
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    POST = "{}/admin/airflow/env/maintenance/repos".format(self.JOBS_API_URL)

    payloadData = {"pipRepository": {"url": "https://pypi.org/simple/"}}
    #"skipCertValidation": true,

    data = json.dumps(payloadData)

    x = requests.post(POST, headers=headers, data=data)

    if x.status_code == 201:
        print("CDE pip repository creation has succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def checkAirflowPythonEnvStatus(self):
    """
    Method to check status of Airflow Python Environment
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
    }

    x = requests.get('{}/admin/airflow/env/maintenance/status'.format(self.JOBS_API_URL), headers=headers)

    if x.status_code == 201:
        print("CDE Airflow Python Environment Status Check has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def buildAirflowPythonEnv(self, pathToRequirementsTxt):
    """
    Method to build the airflow python environment with the provided requirements.txt file
    """

    m = MultipartEncoder(
        fields={
                'file': ('filename', open(pathToRequirementsTxt, 'rb'), 'text/plain')}
        )

    x = requests.post('{}/admin/airflow/env/maintenance/build'.format(self.JOBS_API_URL), data=m,
                      headers={'Authorization': f"Bearer {self.TOKEN}",'Content-Type': m.content_type})

    if x.status_code == 201:
        print("CDE Airflow Python Env Build has succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def activateAirflowPythonEnv(self):
    """
    Method to activate Python Airflow Environment
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
    }

    x = requests.post('{}/admin/airflow/env/maintenance/activate'.format(self.JOBS_API_URL), headers=headers)

    if x.status_code == 201:
        print("CDE Airflow Python Env Activation has succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def deleteAirflowPythonEnv(self):
    """
    Delete Airflow Python Environment
    """

    headers = {
        'accept': 'application/json',
    }

    x = requests.delete('{}/admin/airflow/env'.format(self.JOBS_API_URL), headers=headers)

    if x.status_code == 201:
        print("CDE Airflow Python Env Deletion has succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def deleteMaintenanceSession(self):
    """
    Method to cancel the current Maintenance Session to Manage Airflow Python Environment
    """

    headers = {
        'accept': 'application/json',
    }

    x = requests.delete('{}/admin/airflow/env/maintenance'.format(self.JOBS_API_URL), headers=headers)

    if x.status_code == 201:
        print("CDE Airflow Python Env Deletion has succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def viewMaintenanceSessionLogs(self):
    """
    Method to obtain logs for a Maintenance Session
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
    }

    x = requests.get('{}/admin/airflow/env/maintenance/logs'.format(self.JOBS_API_URL), headers=headers)

    if x.status_code == 201:
        print("CDE Airflow Python Env Deletion has succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def viewEnvironmentLogs(self):
    """
    Method to obtain Environment Logs
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
    }

    x = requests.get('{}/admin/airflow/env/logs'.format(self.JOBS_API_URL), headers=headers)

    if x.status_code == 201:
        print("CDE Airflow Python Env Deletion has succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def getAirflowPythonEnvironmentDetails(self):
    """
    Method to obtain description of airflow python env that is currently handled in the maintenance operation.
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
    }

    x = requests.get('{}/admin/airflow/env'.format(self.JOBS_API_URL), headers=headers)

    if x.status_code == 201:
        print("Current Airflow Python Env Details Request has succeeded\n")
    else:
        print(x.status_code)
        print(x.text)
    
