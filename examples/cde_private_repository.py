############################################
# Author(s): Paul de Fusco                 #
############################################

from cdepy import cdeconnection
from cdepy import cderepositories
from cdepy import cdecredentials
import os
import json

############################################
# Create a Connection to CDE and set Token #
############################################

JOBS_API_URL = "https://9rqklznh.cde-8qhz2284.pdefusco.a465-9q4k.cloudera.site/dex/api/v1"
WORKLOAD_USER = "pauldefusco"
WORKLOAD_PASSWORD = "<pwd>"

myCdeConnection = cdeconnection.CdeConnection(JOBS_API_URL, WORKLOAD_USER, WORKLOAD_PASSWORD)

myCdeConnection.setToken()

############################################
# Create a Basic Credential                #
############################################

myCdeCredentialsManager = cdecredentials.CdeCredentialsManager(myCdeConnection)

credentialName = "myGitCredential"
credentialUsername = "pdefusco"
credentialPassword = "<git-token>"

myCdeCredentials = myCdeCredentialsManager.createBasicCredential(credentialName, credentialUsername, credentialPassword)

myCdeCredentialsManager.listCredentials()

############################################
# Create a Repository with the Credential  #
############################################

myRepoManager = cderepositories.CdeRepositoryManager(myCdeConnection)

repoName = "examplePrivateRepository"
repoPath = "https://github.com/pdefusco/cde_git_repo.git"

myRepoManager.createRepository(repoName, repoPath, repoCredentials=myCdeCredentials,repoBranch="main")

myRepoManager.listRepositories()

myRepoManager.describeRepository(repoName)

myRepoManager.syncRepository(repoName)

myRepoManager.deleteRepository(repoName)
