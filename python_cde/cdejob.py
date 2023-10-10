"""
Module to define CDE Jobs
"""

from abc import ABC, abstractmethod

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

    def __init__(self, type):
        self.type = type

    def createJobDefinition(self):

        pass

class CdeAirflowJobDefinition(CdeJobDefinition):

    """
    Class to define CDE Airflow Jobs
    """

    def __init__(self, type):
        self.type = type

    def createJobDefinition(self):

        pass
