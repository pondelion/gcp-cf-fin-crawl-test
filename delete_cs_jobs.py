import os

from cloud_scheduler_util import (
    get_cloud_scheduler_client,
    delete_job,
    all_job_names,
)


client, project_id = get_cloud_scheduler_client(os.environ['SA_CREDENTIAL_FILEPATH'])
job_names = all_job_names(n=5)
print(job_names)

for i, job_name in enumerate(job_names):
    try:
        delete_job(
            client,
            project_id,
            job_id=job_name,
        )
    except Exception as e:
        print(e)
