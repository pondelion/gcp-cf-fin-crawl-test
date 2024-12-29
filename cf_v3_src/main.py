import base64
import os
from datetime import date, datetime, timedelta, timezone
import requests

from google.cloud import pubsub_v1

from crawl import crawl_yf as crawl
# from crawl import crawl_stooq as crawl


def main(event, context):
     """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     print('v3 start')
     ip = requests.get('https://ipinfo.io/ip').text
     print(f'ip : {ip}')
     JST = timezone(timedelta(hours=+9), 'JST')
     dt_now_jst = datetime.now(JST)
     code_cut_idx = dt_now_jst.hour
     crawl(
          code_cut_idx,
          start_date=date.today()-timedelta(days=4),
          ip=ip,
     )
     print(f'code_cut_idx : {code_cut_idx}')

     print('v3 crawl done')
