############################################
# Author(s): Paul de Fusco                 #
############################################

from cdepy import cdeconnection
from cdepy import cdejob
from cdepy import cdemanager
from cdepy import cderesource
from cdepy import cdeairflowpython
from cdepy.utils import sparkEventLogParser
import os
import json

############################################
# Create a Connection to CDE and set Token #
############################################

JOBS_API_URL = "<myJobsAPIurl>"
WORKLOAD_USER = "<myusername>"
WORKLOAD_PASSWORD = "<mypwd>"

myCdeConnection = cdeconnection.CdeConnection(JOBS_API_URL, WORKLOAD_USER, WORKLOAD_PASSWORD)

myCdeConnection.setToken()

myAirflowPythonEnvManager = cdeairflowpython.CdeAirflowPythonEnv(myCdeConnection)

myAirflowPythonEnvManager.createMaintenanceSession()

myAirflowPythonEnvManager.createPipRepository()

pathToRequirementsTxt = "/examples/requirements.txt"
myAirflowPythonEnvManager.buildAirflowPythonEnv(pathToRequirementsTxt)

myAirflowPythonEnvManager.checkAirflowPythonEnvStatus()

myAirflowPythonEnvManager.viewMaintenanceSessionLogs()

myAirflowPythonEnvManager.activateAirflowPythonEnv()

myAirflowPythonEnvManager.checkAirflowPythonEnvStatus()

myAirflowPythonEnvManager.deleteAirflowPythonEnv()

myAirflowPythonEnvManager.deleteMaintenanceSession()
