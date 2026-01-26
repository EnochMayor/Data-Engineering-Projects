terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project = "annular-text-476712-u9"
  region  = "us-central1"
}

resource "google_storage_bucket" "first-bucket" {
  name          = "annular-text-476712-u9"
  location      = "US"
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}