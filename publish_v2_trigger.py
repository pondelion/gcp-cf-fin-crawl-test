import os
import time

import google.auth
from google.cloud import pubsub_v1


credentials, project_id = google.auth.load_credentials_from_file(os.environ['SA_CREDENTIAL_FILEPATH'])

PROJECT_ID = project_id  #os.environ['PROJECT_ID']
TOPIC_ID = 'cf-fin-crawl-v2-test-topic'
client = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = client.topic_path(PROJECT_ID, TOPIC_ID)
data = f'{0}'.encode()
print(topic_path)
client.publish(topic_path, data=data)

time.sleep(7)