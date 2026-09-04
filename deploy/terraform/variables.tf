variable "project_id" {
  description = "GCP project that owns every resource here."
  type        = string
}

variable "region" {
  description = "Region for Cloud Run, Cloud SQL and the VPC connector."
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment tier. Drives service naming, sampling and sizing."
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod. Local is not deployed."
  }
}

variable "image_tag" {
  description = <<-EOT
    Container image tag to deploy, e.g. a commit SHA.

    Deliberately not defaulted to "latest": a mutable tag makes a rollback
    ambiguous and makes it impossible to say from the Terraform state which
    build is actually running.
  EOT
  type        = string

  validation {
    condition     = var.image_tag != "latest"
    error_message = "image_tag must be immutable. 'latest' cannot be rolled back to a known state."
  }
}

variable "artifact_registry_repository" {
  description = "Artifact Registry repository holding the service images."
  type        = string
  default     = "fluxgate"
}

# --- capacity ----------------------------------------------------------------

variable "ingest_min_instances" {
  description = <<-EOT
    Warm ingest instances.

    Above zero on production tiers: a cold start on the write path means a
    client's batch waits on a container image pull, and telemetry that arrives
    late is worth much less than telemetry that arrives.
  EOT
  type        = number
  default     = 1
}

variable "ingest_max_instances" {
  description = "Ceiling on ingest autoscaling."
  type        = number
  default     = 20
}

variable "aggregator_instances" {
  description = <<-EOT
    Aggregator instances.

    Fixed rather than autoscaled. Each instance holds open windows in memory and
    only writes them when they close, so scaling in mid-window would hand the
    surviving instances a redelivery of everything the departing one had not yet
    flushed. Correct, thanks to the delivery ledger, but pure rework.
  EOT
  type        = number
  default     = 2
}

variable "query_min_instances" {
  description = "Warm query instances. Zero is acceptable: a dashboard tolerates a cold start."
  type        = number
  default     = 0
}

variable "query_max_instances" {
  description = "Ceiling on query autoscaling."
  type        = number
  default     = 10
}

# --- database ----------------------------------------------------------------

variable "database_tier" {
  description = "Cloud SQL machine type."
  type        = string
  default     = "db-custom-2-7680"
}

variable "database_disk_gb" {
  description = "Cloud SQL data disk size. Autoresize is on, so this is a floor."
  type        = number
  default     = 50
}

variable "database_availability_type" {
  description = <<-EOT
    ZONAL or REGIONAL.

    Regional doubles the cost and survives a zone failure. The aggregator holds
    unflushed windows in memory, so a database outage longer than the Pub/Sub
    acknowledgement deadline turns into redelivery and rework rather than data
    loss -- which is why staging can afford zonal and production should not.
  EOT
  type        = string
  default     = "ZONAL"

  validation {
    condition     = contains(["ZONAL", "REGIONAL"], var.database_availability_type)
    error_message = "database_availability_type must be ZONAL or REGIONAL."
  }
}

# --- pipeline ----------------------------------------------------------------

variable "message_retention" {
  description = "How long Pub/Sub keeps unacknowledged messages."
  type        = string
  default     = "86400s" # 24 hours
}

variable "max_delivery_attempts" {
  description = <<-EOT
    Deliveries before a message is dead-lettered.

    Pub/Sub requires 5 to 100. Five distinguishes a transient blip from a
    genuinely undeliverable message without retrying a hopeless one for hours.
  EOT
  type        = number
  default     = 5

  validation {
    condition     = var.max_delivery_attempts >= 5 && var.max_delivery_attempts <= 100
    error_message = "max_delivery_attempts must be between 5 and 100, per the Pub/Sub API."
  }
}

variable "ack_deadline_seconds" {
  description = <<-EOT
    Acknowledgement deadline for the aggregator subscription.

    The client extends this automatically while a handler runs, so it need not
    cover the slowest possible flush -- only the gap before the first extension.
  EOT
  type        = number
  default     = 60
}

variable "alert_notification_channels" {
  description = <<-EOT
    Monitoring notification channel IDs.

    Empty means the alert policies are still created but notify nobody, which is
    the right default for a first apply: a policy that pages a channel that does
    not exist yet fails the apply, and one that pages the wrong people is worse.
  EOT
  type        = list(string)
  default     = []
}

variable "labels" {
  description = "Labels applied to every resource that supports them."
  type        = map(string)
  default     = {}
}

locals {
  # One name prefix everywhere, so a resource can be traced back to its tier at
  # a glance in the console.
  name = "fluxgate-${var.environment}"

  common_labels = merge({
    application = "fluxgate"
    environment = var.environment
    managed-by  = "terraform"
  }, var.labels)

  image_base = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repository}"

  is_production = contains(["staging", "prod"], var.environment)
}
