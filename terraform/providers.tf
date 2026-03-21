terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.24.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone = var.zone
  credentials = file(var.credentials)
}