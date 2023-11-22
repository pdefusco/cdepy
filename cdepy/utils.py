"""
Helper Methods
"""
import requests
import json

def listVcMeta(token):

    headers = {
        'Authorization': f"Bearer {token}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
        }

    x = requests.get(self.JOBS_API_URL+'/info', headers=headers)
    cde_vc_name = json.loads(x.text)["appName"]

    return cdeVcName

def sparkEventLogParser(sparkLogs):
      """
      Method to reformat CDE Spark Event Logs
      Removes unwanted characters from the provided Spark Event Logs
      """

      cleanLogs = sparkLogs.replace("\n", "")

      return cleanLogs
