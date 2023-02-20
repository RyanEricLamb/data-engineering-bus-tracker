variable "project_id" {
  type= string
  description = "Project ID"
}

variable "bucket_name" {
    type =string
    description= "The name of Google Storage Bucket to create"
}

variable "region" {
  type = string
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6"
}

variable "storage_class" {
  type = string
  description = "Storage class type for your bucket."
  default = "STANDARD"
}


variable "registry_id" {
  type = string
  description = "Name of artifact registry repository."
}