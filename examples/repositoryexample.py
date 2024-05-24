############################################
# Author(s): Paul de Fusco                 #
############################################

from cdepy import cdeconnection
from cdepy import cderepositories
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

myRepoManager = cderepositories.CdeRepositoryManager(myCdeConnection)

repoName = "exampleGitRepository"
repoPath = "https://github.com/pdefusco/cde_git_repo.git"

myRepoManager.createRepository(repoName, repoPath, repoBranch="main")

myRepoManager.listRepositories()

myRepoManager.describeRepository(repoName)

filePath = "simple-pyspark-sql.py"
myRepoManager.downloadFileFromRepo(repoName, filePath)

myRepoManager.deleteRepository(repoName)

myRepoManager.listRepositories()
