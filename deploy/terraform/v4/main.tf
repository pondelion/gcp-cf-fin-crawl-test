terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.16.0"
    }
  }
}

provider "google" {
  credentials = "${file(var.credential_filepath)}"
  project = var.project_id
  region  = var.project_region
  zone    = var.project_zone
}

data "archive_file" "function_archive" {
  type        = "zip"
  source_dir  = "../../../cf_v4_src"
  output_path = "./cf_v4_src.zip"
}

resource "google_storage_bucket" "bucket" {
  name          = var.bucket_name
  location      = var.project_region
  storage_class = "STANDARD"
}

resource "google_storage_bucket_object" "packages" {
  name   = "packages/functions.${data.archive_file.function_archive.output_md5}.zip"
  bucket = google_storage_bucket.bucket.name
  source = data.archive_file.function_archive.output_path
}

resource "google_pubsub_topic" "topic" {
  name = "cf-fin-crawl-v4-test-topic"
}

resource "google_cloudfunctions_function" "function" {
  name                  = "cf-fin-crawl-v4-test"
  runtime               = "python310"
  source_archive_bucket = google_storage_bucket.bucket.name
  source_archive_object = google_storage_bucket_object.packages.name
  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.topic.name
  }
  available_memory_mb   = 512
  timeout               = 360
  entry_point           = "main"

  environment_variables = {
    DB_URI = var.DB_URI
    PROJECT_ID = var.PROJECT_ID
  }
}