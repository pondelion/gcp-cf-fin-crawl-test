import base64
import os

from google.cloud import pubsub_v1

from crawl import crawl


def main(event, context):
     """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     code_cut_idx = int(base64.b64decode(event['data']).decode('utf-8'))
     crawl(code_cut_idx)
     print(code_cut_idx)

     if code_cut_idx+1 < int(os.environ.get('N_CODE_CUT', 100)):
     # if code_cut_idx < 3:
          PROJECT_ID = os.environ['PROJECT_ID']
          TOPIC_ID = 'cf-fin-crawl-v2-test-topic'
          client = pubsub_v1.PublisherClient()
          topic_path = client.topic_path(PROJECT_ID, TOPIC_ID)
          data = f'{code_cut_idx+1}'.encode()
          client.publish(topic_path, data=data)
     else:
          print('crawl done')
