






json = {
    "reference": {
                    object (JobReference)
                },
    "placement": {
                    object (JobPlacement)
                },
    "status": {
                    object (JobStatus)
                },
    "statusHistory": [
                {
                    object (JobStatus)
                }
            ],
    "yarnApplications": [
                {
                    object (YarnApplication)
                }
            ],
    "driverOutputResourceUri": string,
    "driverControlFilesUri": string,
    
    # "labels": {
    # string: string,
    # ...
    # },
    
    # "scheduling": {
    # object (JobScheduling)
    # },
    
    "jobUuid": string,
    "done": boolean,
    "driverSchedulingConfig": {
    object (DriverSchedulingConfig)
    },
    "sparkJob": {
    object (SparkJob)
    }
    }
