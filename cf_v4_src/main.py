import base64
import os
from datetime import date, datetime, timedelta, timezone
import requests

from crawl import crawl_yf as crawl
# from crawl import crawl_stooq as crawl


def main(event, context):
     """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     print('v4 start')
     ip = requests.get('https://ipinfo.io/ip').text
     print(f'ip : {ip}')
     # JST = timezone(timedelta(hours=+9), 'JST')
     # dt_now_jst = datetime.now(JST)
     crawl(
          start_datetime=datetime.now()-timedelta(hours=6),
          ip=ip,
     )

     print('v4 crawl done')
