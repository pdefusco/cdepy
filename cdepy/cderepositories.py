"""
Class to manage CDE Repositories
"""

import numpy as np
import pandas as pd
from os.path import exists
from requests_toolbelt import MultipartEncoder
import xmltodict as xd
import pyparsing
import os, json, requests, re, sys
from cdepy.cdeconnection import CdeConnection

class CdeRepositoryManager():
  """
  Class to manage CDE Repositories
  """

  def __init__(self, cdeConnection):
      self.clusterConnection = cdeConnection
      self.JOBS_API_URL = self.clusterConnection.JOBS_API_URL
      self.TOKEN = self.clusterConnection.TOKEN


  def listRepositories(self):
    """
    Method to list all repositories
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    params = (
        ('includeFiles', 'true'),
        ('limit', '100'),
        ('offset', '0'),
        ('orderby', 'name'),
        ('orderasc', 'true'),
    )

    x = requests.get('{}/repositories'.format(self.JOBS_API_URL), headers=headers, params=params)

    return x.text

    if x.status_code == 201:
        print("Listing CDE Repositories has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def createRepository(self, repoName, repoPath, repoCredentialName=None, repoBranch="main"):
    """
    Method to create a repository
    """

    repoDefinition = {
      "git": {
        "branch": repoBranch,
        "repository": repoPath,
        "credential": repoCredentialName
      },
      "name": repoName,
      "skipCredentialValidation": True
    }

    data = json.dumps(repoDefinition)

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    x = requests.post('{}/repositories'.format(self.JOBS_API_URL), headers=headers, data=data)

    if x.status_code == 201:
        print("CDE Repository Creation has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def describeRepository(self, repoName):
    """
    Method to describe a repository
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    params = (
        ('includeFiles', 'true'),
    )

    x = requests.get('{0}/repositories/{1}'.format(self.JOBS_API_URL, repoName), headers=headers, params=params)
    return x.text

    if x.status_code == 201:
        print("CDE Repository Description has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def pullRepository(self, repoName):
    """
    Method to pull latest commit from repository
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    x = requests.post('{0}/repositories/{1}'.format(self.JOBS_API_URL, repoName), headers=headers)

    if x.status_code == 201:
        print("CDE Repository Description has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def deleteRepository(self, repoName):
    """
    Method to delete a repository in CDE
    """

    headers = {
        'Authorization': f"Bearer {self.TOKEN}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    x = requests.delete('{0}/repositories/{1}'.format(self.JOBS_API_URL, repoName), headers=headers)

    if x.status_code == 201:
        print("CDE Repository Description has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)


  def downloadFileFromRepo(self, repoName, filePath):
    """
    Method to download a file from a CDE repository
    """

    headers = {
    'Authorization': f"Bearer {self.TOKEN}",
    'accept': 'application/octet-stream',
    'Content-Type': 'application/json'
    }

    x = requests.get('{}/repositories/{}/{}'.format(self.JOBS_API_URL, repoName, filePath), headers=headers)

    return x.content

    if x.status_code == 201:
        print("CDE Repository Description has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)

  def syncRepository(self, repoName):
    """
    Method to sync a cde repo with its git origin
    """

    headers = {
    'Authorization': f"Bearer {self.TOKEN}",
    'accept': 'application/octet-stream',
    'Content-Type': 'application/json'
    }

    x = requests.post('{}/repositories/{}'.format(self.JOBS_API_URL, repoName), headers=headers)

    return x.content

    if x.status_code == 201:
        print("CDE Repository Sync with Git Origin has Succeeded\n")
    else:
        print(x.status_code)
        print(x.text)
