import time
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from cloud_scheduler_util import (
    get_cloud_scheduler_client,
    pause_job,
)


client, project_id = get_cloud_scheduler_client(os.environ['SA_CREDENTIAL_FILEPATH'])

try:
    pause_job(
        client,
        project_id,
        job_id='cf-fin-crawl-v2-test-job',
    )
    time.sleep(5)
except Exception as e:
    print(e)
