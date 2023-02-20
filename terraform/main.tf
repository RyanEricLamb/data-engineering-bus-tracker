terraform {
    required_version = ">=1.0"
    backend "local" {} #Can change from local to "gcs" or "s3" for google/aws
    required_providers {
        google = {
            source = "hashicorp/google"
        }
    }
}

provider "google" {
    project = var.project_id
    region = var.region
    //credentials = file(var.credentials) #use this if you don't want to set env-var GOOGLE_APPLICATION
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "bus_data_lake" {
  name          = var.bucket_name
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  force_destroy = true
}

#Artifact registry for containers
resource "google_artifact_registry_repository" "bus-container-registry" {
  location      = var.region
  repository_id = var.registry_id
  format        = "DOCKER"
}