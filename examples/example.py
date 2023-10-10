############################################
# Author(s): Paul de Fusco                 #
############################################

from cdepy import cdeconnection
from cdepy import cdejob
from cdepy import cdemanager
from cdepy import cderesource

############################################
# Create a Connection to CDE and set Token #
############################################

JOBS_API_URL = "https://<YOUR-CLUSTER>.cloudera.site/dex/api/v1"
WORKLOAD_USER = "<Your-CDP-Workload-User>"
WORKLOAD_PASSWORD = "<Your-CDP-Workload-Password>"

myCdeConnection = cdeconnection.CdeConnection(JOBS_API_URL, WORKLOAD_USER, WORKLOAD_PASSWORD)

myCdeConnection.setToken()

############################################
# Create a CDE Files Resource Definition   #
############################################

CDE_RESOURCE_NAME = "myFilesCdeResource"
myCdeFilesResource = cderesource.CdeFilesResource(CDE_RESOURCE_NAME)
myCdeFilesResourceDefinition = myCdeFilesResource.createResourceDefinition()

############################################
# Create a CDE Spark Job  Definition       #
############################################

CDE_JOB_NAME = "myCdeSparkJob"
APPLICATION_FILE_NAME = "pysparksql.py"

myCdeSparkJob = cdejob.CdeSparkJob(myCdeConnection)
myCdeSparkJobDefinition = myCdeSparkJob.createJobDefinition(CDE_JOB_NAME, CDE_RESOURCE_NAME, APPLICATION_FILE_NAME)

############################################
# Create Resource and Job in CDE Cluster   #
############################################

LOCAL_FILE_PATH = "examples"
LOCAL_FILE_NAME = "pysparksql.py"

myCdeClusterManager = cdemanager.CdeClusterManager(myCdeConnection)

### CREATE FILES RESOURCE AND UPLOAD PYSPARK SCRIPT TO IT ###

myCdeClusterManager.createResource(myCdeFilesResourceDefinition)
myCdeClusterManager.uploadFile(CDE_RESOURCE_NAME, LOCAL_FILE_PATH, LOCAL_FILE_NAME)

### CREATE SPARK JOB AND RUN IT ###

myCdeClusterManager.createJob(myCdeSparkJobDefinition)
myCdeClusterManager.runJob(CDE_JOB_NAME)

### VALIDATE JOB RUNS ###

myCdeClusterManager.listJobRuns()
