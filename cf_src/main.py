import base64

# from crawl import crawl_yf as crawl
from crawl import crawl_stooq as crawl


def main(event, context):
     """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     code_cut_idx = int(base64.b64decode(event['data']).decode('utf-8'))
     crawl(code_cut_idx)
     print(f'code_cut_idx : {code_cut_idx}')
