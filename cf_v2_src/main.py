import base64
import os
from datetime import date, datetime, timedelta
import requests

from google.cloud import pubsub_v1

from crawl import crawl_yf as crawl
# from crawl import crawl_stooq as crawl


# def main(event, context):
#      """Triggered from a message on a Cloud Pub/Sub topic.
#      Args:
#           event (dict): Event payload.
#           context (google.cloud.functions.Context): Metadata for the event.
#      """
#      code_cut_idx = int(base64.b64decode(event['data']).decode('utf-8'))
#      crawl(code_cut_idx)
#      print(f'code_cut_idx : {code_cut_idx}')

#      if code_cut_idx+1 < int(os.environ.get('N_CODE_CUT', 100)):
#      # if code_cut_idx < 3:
#           PROJECT_ID = os.environ['PROJECT_ID']
#           TOPIC_ID = 'cf-fin-crawl-v2-test-topic'
#           client = pubsub_v1.PublisherClient()
#           topic_path = client.topic_path(PROJECT_ID, TOPIC_ID)
#           data = f'{code_cut_idx+1}'.encode()
#           client.publish(topic_path, data=data)
#      else:
#           print('crawl done')


def main(event, context):
     """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     ip = requests.get('https://ipinfo.io/ip').text
     print(f'ip : {ip}')
     code_cut_idx = datetime.now().hour
     crawl(
          code_cut_idx,
          n_code_cut=24,
          start_date=date.today()-timedelta(days=4),
          ip=ip,
     )
     print(f'code_cut_idx : {code_cut_idx}')

     print('crawl done')
