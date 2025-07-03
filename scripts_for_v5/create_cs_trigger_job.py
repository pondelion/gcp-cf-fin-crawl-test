import time
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from cloud_scheduler_util import (
    get_cloud_scheduler_client,
    create_job,
    delete_job,
)


client, project_id = get_cloud_scheduler_client(os.environ['SA_CREDENTIAL_FILEPATH'])
job_id = 'cf-fin-crawl-v5-test-job'

try:
    delete_job(client, project_id, job_id=job_id)
    time.sleep(5)
    print(f'deleted alredy set job {job_id}')
except Exception as e:
    print(e)

try:
    create_job(
        client,
        project_id,
        job_id=job_id,
        # Set cron to run daily at 22:00 UTC = 17:00 EST (market closed 1 hour ago)
        schedule='0 22,23 * * *',
        pubsub_target_topic_name=f'projects/{project_id}/topics/cf-fin-crawl-v5-test-topic',
        pubsub_target_data=f'0',
        timezone='UTC',
    )
    time.sleep(5)
except Exception as e:
    print(e)
