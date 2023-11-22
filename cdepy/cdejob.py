"""
Module to define CDE Jobs
"""

from abc import ABC, abstractmethod
from cdepy.cdeconnection import CdeConnection

class CdeJob(ABC):
    """
    Class to define CDE Job
    """
    @abstractmethod
    def createJobDefinition(self):
        pass

    @abstractmethod
    def hasKey(self):
        pass


class CdeSparkJob(CdeJob):
    """
    Class to define CDE Spark Jobs
    """

    def __init__(self, cdeConnection):
        self.clusterConnection = cdeConnection
        self.WORKLOAD_USER = self.clusterConnection.WORKLOAD_USER

    def hasKey(self, d, key, input_val, cdeSparkJobDefinition):
        try:
            if key in d:
                val = d[key]
                print("Spark Conf {} is found".format(key))
                if val == None:
                    cdeSparkJobDefinition[key] = input_val
                    print("Spark Conf {} is added".format(key))
                    return cdeSparkJobDefinition
                else:
                    cdeSparkJobDefinition[val][key] = input_val
                    print("Spark Conf {} is added".format(key))
                    return cdeSparkJobDefinition
        except:
            print("\nError Processing Option: ", key)
        else:
            print("\nProvided Option Not Supported: ", key)
            print("\nPlease try again.")

    def createJobDefinition(self, CDE_JOB_NAME, CDE_RESOURCE_NAME, APPLICATION_FILE_NAME, SPARK_CONFS={"spark.pyspark.python": "python3"}, **kwargs):
        """
        Method to create CDE Spark Job Definition
        Requires CDE Job Name, CDE Files Resource Name,
        Application File Name, and optionally spark configs
        """

        cdeSparkJobDefinition = {
              "name": CDE_JOB_NAME, #CDE Job Name As you want it to appear in the CDE JOBS UI
              "type": "spark",
              "retentionPolicy": "keep_indefinitely",
              "mounts": [
                {
                  "resourceName": CDE_RESOURCE_NAME
                }
              ],
              "spark": {
                "logLevel": "INFO",
                "file": APPLICATION_FILE_NAME
              },
              "schedule": {
                "enabled": False
              }
            }

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

        for param in kwargs:
            print(param)
            cdeSparkJobDefinition = self.hasKey(sparkConfigs, param, kwargs[param], cdeSparkJobDefinition)

        return cdeSparkJobDefinition


class CdeAirflowJob(CdeJob):
    """
    Class to define CDE Airflow Jobs
    """

    def __init__(self, cdeConnection):
        self.clusterConnection = cdeConnection
        self.WORKLOAD_USER = self.clusterConnection.WORKLOAD_USER

    def hasKey(d, key, input_val, cdeAirflowJobDefinition):
        try:
            if key in d:
                val = d[key]
                if val == None:
                    cdeAirflowJobDefinition[key] = input_val
                    return cdeAirflowJobDefinition
                else:
                    cdeAirflowJobDefinition[val][key] = input_val
                    return cdeAirflowJobDefinition
        except:
            print("Error Processing Option: ", key)
        else:
            print("Provided Option Not Supported: ", key)

    def createJobDefinition(self, CDE_JOB_NAME, DAG_FILE, FILE_MOUNTS=None):
        """
        Method to create CDE Job Definition of type Airflow
        Requires CDE Job Name, Application File Name and optionally CDE Files Resource Name
        """

        cdeAirflowJobDefinition = {
              "name": CDE_JOB_NAME,# CDE Job Name As you want it to appear in the CDE JOBS UI
              "type": "airflow",
              "retentionPolicy": "keep_indefinitely",
              "dagFile": DAG_FILE
            }

        if FILE_MOUNTS != None and isinstance(FILE_MOUNTS, dict):
            cdeAirflowJobDefinition[fileMounts] = FILE_MOUNTS

        return cdeAirflowJobDefinition
