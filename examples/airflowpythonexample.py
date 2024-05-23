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

JOBS_API_URL = "https://jcxk6ghn.cde-ntvvr5hx.go01-dem.ylcu-atmi.cloudera.site/dex/api/v1"
WORKLOAD_USER = "pauldefusco"
WORKLOAD_PASSWORD = "Paolino1987!"

myCdeConnection = cdeconnection.CdeConnection(JOBS_API_URL, WORKLOAD_USER, WORKLOAD_PASSWORD)

myCdeConnection.setToken()

myAirflowPythonEnvManager = cdeairflowpython.CdeAirflowPythonEnv(myCdeConnection)

myAirflowPythonEnvManager.createMaintenanceSession()

myAirflowPythonEnvManager.createPipRepository()

myAirflowPythonEnvManager.checkAirflowPythonEnvStatus()
# STATUS SHOULD BE {"status":"pip-repos-defined"}

pathToRequirementsTxt = "/home/cdsw/examples/requirements.txt"
myAirflowPythonEnvManager.buildAirflowPythonEnv(pathToRequirementsTxt)

myAirflowPythonEnvManager.checkAirflowPythonEnvStatus()
# RESPONSE STATUS SHOULD BE {"status":"building"}
# AFTER 2 MINUTES REPEAT THE REQUEST. RESPONSE STATUS SHOULD EVENTUALLY BE {"status":"built"}

myAirflowPythonEnvManager.getAirflowPythonEnvironmentDetails()

myAirflowPythonEnvManager.viewMaintenanceSessionLogs()

myAirflowPythonEnvManager.activateAirflowPythonEnv()

myAirflowPythonEnvManager.checkAirflowPythonEnvStatus()
# AT FIRST RESPONSE STATUS SHOULD BE {"status":"activating"}
# AFTER 2 MINUTES REPEAT THE REQUEST. RESPONSE STATUS SHOULD EVENTUALLY BE

myAirflowPythonEnvManager.deleteAirflowPythonEnv()

myAirflowPythonEnvManager.deleteMaintenanceSession()
