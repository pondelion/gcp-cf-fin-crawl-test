from datetime import datetime, timedelta
import os

from cloud_scheduler_util import (
    get_cloud_scheduler_client,
    create_job,
    all_job_names,
)


client, project_id = get_cloud_scheduler_client(os.environ['SA_CREDENTIAL_FILEPATH'])
job_names = all_job_names(n=5)
print(job_names)

base_dt = datetime(2022, 1, 1, 0, 1)

for i, job_name in enumerate(job_names):
    dt = base_dt + timedelta(minutes=i)
    hour = dt.hour
    minute = dt.minute
    create_job(
        client,
        project_id,
        job_id=job_name,
        schedule=f'{minute} {hour} * * *',
        pubsub_target_topic_name=f'projects/{project_id}/topics/cf-fin-crawl-test-topic',
        pubsub_target_data=f'{i}',
    )
