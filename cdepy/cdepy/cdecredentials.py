"""
Class to manage CDE Credentials
"""

import numpy as np
import pandas as pd
from os.path import exists
from requests_toolbelt import MultipartEncoder
import xmltodict as xd
import pyparsing
import os, json, requests, re, sys
from cdepy.cdeconnection import CdeConnection

class CdeCredentialsManager():
  """
  Class to manage CDE Credentials
  """

  def __init__(self, cdeConnection):
      self.clusterConnection = cdeConnection
      self.JOBS_API_URL = self.clusterConnection.JOBS_API_URL
      self.TOKEN = self.clusterConnection.TOKEN


  def listCredentials(self):
    """
    Method to list all credentials
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    params = (
        ('limit', '100'),
        ('offset', '0'),
        ('orderby', 'name'),
        ('orderasc', 'true'),
    )

    x = requests.get('{}/credentials'.format(self.JOBS_API_URL), headers=headers, params=params)

    return x.text

    if x.status_code == 201:
        print("Listing CDE Repositories has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def createBasicCredential(self, credentialName, credentialUsername, credentialPassword):
    """
    Method to create a credential
    """

    repoDefinition = {
      "basic": {
        "username": credentialUsername,
        "password": credentialPassword
      },
      "name": credentialName,
      "skipCredentialValidation": True,
      "type": "basic"
    }

    data = json.dumps(repoDefinition)

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    x = requests.post('{}/credentials'.format(self.JOBS_API_URL), headers=headers, data=data)

    if x.status_code == 201:
        print("CDE Credential Creation has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def describeCredential(self, credentialName):
    """
    Method to describe a credential
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    params = (
        ('includeFiles', 'true'),
    )

    x = requests.get('{0}/credentials/{1}'.format(self.JOBS_API_URL, credentialName), headers=headers, params=params)

    if x.status_code == 201:
        print("CDE Credential Description has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def deleteCredential(self, credentialName):
    """
    Method to delete a repository in CDE
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    x = requests.delete('{0}/credentials/{1}'.format(self.JOBS_API_URL, credentialName), headers=headers)

    if x.status_code == 201:
        print("CDE Credential Deletion has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def updateBasicCredential(self, credentialName, credentialUsername, credentialPassword):
    """
    Method to update a basic CDE Credential
    """
    raise NotImplementedError
