terraform {
  required_version = ">= 1.9"

  required_providers {
    google = {
      source = "hashicorp/google"
      # Pinned to a minor range rather than floating: a provider upgrade can
      # change a resource's default and produce a diff nobody asked for, which
      # is a poor thing to discover during an unrelated deploy.
      version = "~> 6.20"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State lives in GCS, not on a laptop. Two engineers applying from local
  # state is how infrastructure ends up in a condition neither of them can
  # explain. The bucket is created out of band -- Terraform cannot create the
  # bucket that holds its own state.
  #
  #   terraform init \
  #     -backend-config="bucket=fluxgate-tfstate-<project>" \
  #     -backend-config="prefix=fluxgate/<env>"
  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}
