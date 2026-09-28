# First apply creates infrastructure only. No runtime can receive traffic until
# the operator has executed the migration job and selected a key version.
resource "google_project_service" "required" {
  for_each = toset([
    "compute.googleapis.com", "run.googleapis.com", "sqladmin.googleapis.com",
    "servicenetworking.googleapis.com", "vpcaccess.googleapis.com",
    "pubsub.googleapis.com", "secretmanager.googleapis.com", "iam.googleapis.com",
    "artifactregistry.googleapis.com", "monitoring.googleapis.com", "logging.googleapis.com",
  ])
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "images" {
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_registry_repository
  format        = "DOCKER"
  labels        = local.common_labels
  docker_config { immutable_tags = true }
  depends_on = [google_project_service.required]
}

resource "terraform_data" "deployment_gate" {
  lifecycle {
    precondition {
      condition     = !var.deploy_services || (alltrue([for image in values(var.images) : image != ""]) && var.api_keys_version != "")
      error_message = "Before enabling services: build all images, run the migration job, and populate/pin API keys."
    }
    precondition {
      condition     = var.environment != "prod" || !var.deploy_services || (var.database_availability_type == "REGIONAL" && var.ingest_min_instances >= 1 && length(var.alert_notification_channels) > 0)
      error_message = "Production services require regional SQL, warm ingestion, and an alert notification channel."
    }
    precondition {
      condition     = alltrue([for n in [var.ingest_min_instances, var.ingest_max_instances, var.query_min_instances, var.query_max_instances, var.aggregator_instances, var.database_pool_connections, var.database_max_connections] : n == floor(n) && n >= 0]) && var.aggregator_instances >= 1 && var.database_pool_connections >= 1 && var.ingest_max_instances >= max(1, var.ingest_min_instances) && var.query_max_instances >= max(1, var.query_min_instances)
      error_message = "Replica and connection limits must be whole numbers with positive maxima and valid minimums."
    }
    precondition {
      # Reserve space for a complete old/new revision overlap plus administration.
      condition     = 2 * (var.ingest_max_instances + var.query_max_instances + var.aggregator_instances) * var.database_pool_connections + 20 <= var.database_max_connections
      error_message = "Database capacity must cover two revisions at maximum scale plus 20 administration connections."
    }
    precondition {
      condition     = can(regex("^[0-9]+s$", var.message_retention)) && try(tonumber(trimsuffix(var.message_retention, "s")) >= 600 && tonumber(trimsuffix(var.message_retention, "s")) <= 2678400, false)
      error_message = "message_retention must be 600s through 2678400s (31 days)."
    }
    precondition {
      condition     = var.ack_deadline_seconds >= 10 && var.ack_deadline_seconds <= 600 && var.ack_deadline_seconds == floor(var.ack_deadline_seconds)
      error_message = "ack_deadline_seconds must be an integer from 10 to 600."
    }
  }
}
