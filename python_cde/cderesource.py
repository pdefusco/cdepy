"""
Module to define CDE Resources
"""

from abc import ABC, abstractmethod

class CdeResourceDefinition(ABC):

    """
    Class to define CDE Job
    """
    @abstractmethod
    def createJobDefinition(self):
        pass


class CdeFilesResourceDefinition(CdeResourceDefinition):

    """
    Class to define CDE Spark Jobs
    """

    def __init__(self, type):
        self.type = type

    def createJobDefinition(self):

        pass

class CdePythonResourceDefinition(CdeResourceDefinition):

    """
    Class to define CDE Airflow Jobs
    """

    def __init__(self, type):
        self.type = type

    def createJobDefinition(self):

        pass
