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
  default = "gcp_credentials.jsons" #suppose you left your json file in the terraform folder
}

variable "location" {
  type = string
}

variable bucket_name {
  type = string
  default = "${var.project}-bronze-data"
}

variable service_account_email {
  type = string
  default = "${var.service_account_email}"
}