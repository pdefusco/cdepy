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


class CdeConnection:
    """
    Class to establish a connection to a CDE Virtual Cluster
    """

    def __init__(self, JOBS_API_URL, WORKLOAD_USER, WORKLOAD_PASSWORD, TOKEN=None):
        self.JOBS_API_URL = JOBS_API_URL
        self.WORKLOAD_USER = WORKLOAD_USER
        self.WORKLOAD_PASSWORD = WORKLOAD_PASSWORD
        self.TOKEN = TOKEN
        #self.GMAIL_APP_PASSWORD = GMAIL_APP_PASSWORD

    def setToken(self):
        """
        Method to set user token to interact with CDE Service remotely
        """

        rep = self.JOBS_API_URL.split("/")[2].split(".")[0]
        GET_TOKEN_URL = self.JOBS_API_URL.replace(rep, "service").replace("dex/api/v1", "gateway/authtkn/knoxtoken/api/v1/token")

        token_json = requests.get(GET_TOKEN_URL, auth=(self.WORKLOAD_USER, self.WORKLOAD_PASSWORD))

        print("TOKEN SET SUCCESSFULLY\n")
        self.TOKEN = json.loads(token_json.text)["access_token"]
