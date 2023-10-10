"""
Module to define CDE Jobs
"""

from abc import ABC, abstractmethod
from cdeconnection import CdeConnection

class CdeJobDefinition(ABC):

    """
    Class to define CDE Job
    """
    @abstractmethod
    def createJobDefinition(self):
        pass


class CdeSparkJobDefinition(CdeJobDefinition):

    """
    Class to define CDE Spark Jobs
    """

    def __init__(self, cdeConnection, cdeSparkJobDefinition):
        self.cdeSparkJobDefinition = cdeSparkJobDefinition
        self.clusterConnection = cdeConnection
        self.WORKLOAD_USER = self.clusterConnection.WORKLOAD_USER

    def setSparkJobDefinition(self, CDE_JOB_NAME, CDE_RESOURCE_NAME, APPLICATION_FILE_NAME, SPARK_CONFS={"spark.pyspark.python": "python3"}):
        """
        Method to create CDE Spark Job Definition
        Requires CDE Job Name, CDE Files Resource Name, Application File Name, and optionally spark configs
        """

        print("Setting CDE Spark Job Definition in Progress\n")

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

        cdeSparkJobDefinition = {
              "name": CDE_JOB_NAME,# CDE Job Name As you want it to appear in the CDE JOBS UI
              "type": "spark",
              "retentionPolicy": "keep_indefinitely",
              "mounts": [
                {
                  "resourceName": CDE_RESOURCE_NAME
                }
              ],
              "spark": {
                "file": APPLICATION_FILE_NAME,
                "driverMemory": "1g",
                "driverCores": 1, #this must be an integer
                "executorMemory": "4g",
                "executorCores": 1, #this must be an integer
                "conf": SPARK_CONFS,
                "logLevel": "INFO"
              },
              "schedule": {
                "enabled": False,
                "user": self.WORKLOAD_USER #Your CDP Workload User is automatically set by CML as an Environment Variable
              }
            }

        return cdeSparkJobDefinition


class CdeAirflowJobDefinition(CdeJobDefinition):

    """
    Class to define CDE Airflow Jobs
    """

    def __init__(self, cdeConnection, cdeAirflowJobDefinition=None):
        self.cdeAirflowJobDefinition = cdeAirflowJobDefinition
        self.clusterConnection = cdeConnection
        self.WORKLOAD_USER = self.clusterConnection.WORKLOAD_USER

    def setJobDefinition(self, CDE_JOB_NAME, DAG_FILE):
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

        return cdeAirflowJobDefinition
