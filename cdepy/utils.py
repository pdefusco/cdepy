"""
Helper Methods
"""
import requests
import json

def sparkEventLogParser(sparkLogs):
      """
      Method to reformat CDE Spark Event Logs
      Removes unwanted characters from the provided Spark Event Logs
      """

      cleanLogs = sparkLogs.replace("\n", "")

      return cleanLogs

def listAvailableConfs(jobType="Spark"):
    """
    Method to list available Spark or Airflow Configurations
    that user can choose from
    when creating a CDE Spark Job Definition
    with createJobDefinition method of cdejob module
    """

    if jobType=="Spark":

        sparkConfigs = {
            "alertAfterDuration": "alerting",
            "emailOnFailure": "alerting",
            "emailOnSLAMiss": "alerting",
            "mailTo": "alerting",
            "dataConnectors": None,
            "dirPrefix":"mounts",
            "resourceName": "mounts",
            "name": "string",
            "retentionPolicy": None,
            "runtimeImageResourceName": None,
            "catchup": "schedule",
            "cronExpression": "schedule",
            "dependsOnPast": "schedule",
            "enabled": "schedule",
            "end": "schedule",
            "nextExecution": "schedule",
            "paused": "schedule",
            "pausedUponCreation": "schedule",
            "start": "schedule",
            "user": "schedule",
            "args": "spark",
            "className": "spark",
            "driverCores": "spark",
            "driverMemory": "spark",
            "executorCores": "spark",
            "executorMemory": "spark",
            "files": "spark",
            "jars": "spark",
            "logLevel": "spark",
            "name": "spark",
            "numExecutors": "spark",
            "proxyUser": "spark",
            "pyFiles": "spark",
            "pythonEnvResourceName": "spark",
            "type": "spark",
            "workloadCredentials":"spark"}

        print(sparkConfigs.keys())

    elif jobType == "Airflow":
        pass

    else:
        print("Error. Please enter jobType of either 'Airflow' or 'Spark'")
