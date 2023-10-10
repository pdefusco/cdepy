"""
Helper Methods
"""
import requests
import json

def listVcMeta(self, token):

    headers = {
        'Authorization': f"Bearer {token}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
        }

    x = requests.get(self.JOBS_API_URL+'/info', headers=headers)
    cde_vc_name = json.loads(x.text)["appName"]
    #cde_vc_id = json.loads(x.text)["appId"]
    #cde_vc_console_url = json.loads(x.text)["cdeConsoleURL"]
    #cde_cluster_id = json.loads(x.text)["clusterID"]
    #cde_version = json.loads(x.text)["version"]

    return cdeVcName

def runSparkJob(self, token, cde_job_name, driver_cores = 2, driver_memory = "4g", executor_cores = 4, executor_memory = "4g", num_executors = 4):

    print("Started to Submit Spark Job {}".format(cde_job_name))

    cde_payload = {"overrides":
                   {"spark":
                    {"driverCores": driver_cores,
                     "driverMemory": driver_memory,
                     "executorCores": executor_cores,
                     "executorMemory": executor_memory,
                     "numExecutors": num_executors}
                   }
                  }

    headers = {
        'Authorization': f"Bearer {token}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    POST = "{}/jobs/".format(self.JOBS_API_URL)+cde_job_name+"/run"

    data = json.dumps(cde_payload)

    x = requests.post(POST, headers=headers, data=data)

    if x.status_code == 201:
        print("Submitting CDE Spark Job {} has Succeeded".format(cde_job_name))
        print("This doesn't necessarily mean that the CDE Spark Job has Succeeded")
        print("Please visit the CDE Job Runs UI to check on CDE Job Status")
    else:
        print(x.status_code)
        print(x.text)
