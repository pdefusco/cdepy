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


class CdeRepository():
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

    pass


  def createRepository(self, name):
    """
    Method to create a repository
    """

    pass


  def describeRepository(self, name):
    """
    Method to describe a repository
    """

    pass


  def pullRepository(self, name):
    """
    Method to pull latest commit from repository
    """

    pass


  def deleteRepository(self, name):
    """
    Method to delete a repository in CDE
    """

    pass


  def downloadFileFromRepo(self, name, filepath):
    """
    Method to download a file from a CDE repository
    """

    pass
