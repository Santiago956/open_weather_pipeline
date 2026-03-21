resource "google_storage_bucket" "weather_data_bucket" {
  name     = "${var.project}-${var.bucket_name}"
  location = var.location

  storage_class = "STANDARD"
  public_access_prevention = "enforced"
  uniform_bucket_level_access = true 



  force_destroy = true

  lifecycle_rule {
    condition {
      age = 90 #delete the bucket after 3 months
    }
    action {
      type = "Delete"
    }
  }
  versioning {
    enabled = true # bucket versioning in case of accidental deletion
  }
}

resource "google_bigquery_dataset" "weather_data_dataset" {
  dataset_id = "weather_data"
  project    = var.project
  location   = var.location
  friendly_name = "Weather Data Warehouse"
  description = "Dataset for storing weather data with silver and gold layers"

  delete_contents_on_destroy = true # delete dataset contents when the dataset is destroyed
  default_table_expiration_ms = 2592000000 #30 days in milliseconds, necessary for free tier accounts
  default_partition_expiration_ms = 2592000000 #30 days in milliseconds, necessary for free tier accounts

  labels = {
    env = "dev"
    project = "zoomcamp"
    purpose = "portifolio"
  }
}

# Exemplo: Garante que a Service Account possa ler/escrever no Bucket
resource "google_storage_bucket_iam_member" "editor" {
  bucket = google_storage_bucket.weather_data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.service_account_email}"
}