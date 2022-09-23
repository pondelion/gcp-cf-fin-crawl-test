import google.auth
from google.cloud import scheduler_v1
from google.cloud.scheduler_v1 import PubsubTarget


def get_cloud_scheduler_client(sa_credential_filepath):
    credentials, project_id = google.auth.load_credentials_from_file(sa_credential_filepath)
    client = scheduler_v1.CloudSchedulerClient(credentials=credentials)
    return client, project_id


def get_job_list(cs_client, project_id, region='asia-northeast1'):
    request = scheduler_v1.ListJobsRequest(parent = f"projects/{project_id}/locations/{region}")
    page_result = cs_client.list_jobs(request=request)
    return [r.name for r in page_result]


def create_job(cs_client, project_id, job_id, schedule, pubsub_target_topic_name, pubsub_target_data, timezone='Asia/Tokyo', description='', location='asia-northeast1'):
    # parent= cs_client.location_path(project_id, location)
    parent = f'projects/{project_id}/locations/{location}'
    job_name = f'projects/{project_id}/locations/{location}/jobs/{job_id}'
    pt = PubsubTarget(
        topic_name=pubsub_target_topic_name,
        data=pubsub_target_data.encode('utf-8'),
    )
    job_dict = {
        'name': job_name,
        'pubsub_target': pt,
        'schedule': schedule,
        'time_zone': timezone,
        'description': description
    }
    job = cs_client.create_job(parent=parent, job=job_dict)


def delete_job(cs_client, project_id, job_id, location='asia-northeast1'):
    job_name = f'projects/{project_id}/locations/{location}/jobs/{job_id}'
    request = scheduler_v1.DeleteJobRequest(
        name=job_name,
    )
    cs_client.delete_job(request=request)


def pause_job(cs_client, project_id, job_id, location='asia-northeast1'):
    job_name = f'projects/{project_id}/locations/{location}/jobs/{job_id}'
    request = scheduler_v1.PauseJobRequest(
        name=job_name,
    )
    cs_client.pause_job(request=request)


def job_name(idx: int):
    return f'cf-fin-crawl-test-job{idx}'


def all_job_names(n: int):
    return [job_name(idx) for idx in range(n)]
