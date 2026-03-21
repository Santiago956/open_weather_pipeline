# variables.tf
variable "project" {
  type = string
}

variable "zone" {
  type = string
}

variable "region" {
  type = string
}

variable "credentials" {
  description = "Json path to your service account credentials"
  type = string
  default = "gcp_credentials.json" #suppose you left your json file in the terraform folder
}

variable "location" {
  type = string
}

variable bucket_name {
  type = string
  default = "weather-bronze-data"
}

variable service_account_email {
  description = "Service account email"
  type = string
}