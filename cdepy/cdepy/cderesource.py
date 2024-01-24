"""
Module to define CDE Resources
"""

from abc import ABC, abstractmethod

class CdeResource(ABC):
    """
    Abstract Class to define CDE Resource
    """
    @abstractmethod
    def createResourceDefinition(self):
        raise NotImplementedError

class CdeFilesResource(CdeResource):
    """
    Class to define CDE Resource of type Files
    """

    def __init__(self, CDE_RESOURCE_NAME):
        self.CDE_RESOURCE_NAME = CDE_RESOURCE_NAME

    def createResourceDefinition(self):
        """
        Method to create a CDE Resource Definition for CDE Resources of type Files
        """

        cdeFilesResourceDefinition = {
                                    "name": str(self.CDE_RESOURCE_NAME),
                                    "type":"files",
                                    "retention-policy" : "keep_indefinitely"
                                    }

        return cdeFilesResourceDefinition

class CdePythonResource(CdeResource):
    """
    Class to define CDE Resource or type Python Environment
    """

    def __init__(self, type):
        self.type = type

    def createResourceDefinition(self, CDE_RESOURCE_NAME):
        """
        Method to create a CDE Resource Definition for CDE Resources of type Python Environment
        """

        cdePythonResourceDefinition = {
                                    "name": str(CDE_RESOURCE_NAME),
                                    "type": "python-env",
                                    "python-version": "python3",
                                    "retention-policy" : "keep_indefinitely"
                                    }


        return cdePythonResourceDefinition
