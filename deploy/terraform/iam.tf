# Service identities, one per service, each granted only what that service
# actually does.
#
# The temptation is one account with roles/editor and a note to tighten it
# later. The reason not to is concrete: the ingest API accepts arbitrary input
# from the public internet, and if it is ever compromised the blast radius is
# exactly the permissions it holds. It publishes to one topic. That is all it
# should be able to do.

resource "google_service_account" "ingest" {
  account_id   = "${local.name}-ingest"
  display_name = "Fluxgate ingest API (${var.environment})"
  description  = "Publishes validated batches. No database access, no subscribe."
  project      = var.project_id
}

resource "google_service_account" "aggregator" {
  account_id   = "${local.name}-aggregator"
  display_name = "Fluxgate aggregator (${var.environment})"
  description  = "Consumes batches and writes rollups. No publish rights."
  project      = var.project_id
}

resource "google_service_account" "query" {
  account_id   = "${local.name}-query"
  display_name = "Fluxgate query API (${var.environment})"
  description  = "Reads rollups. No Pub/Sub access of any kind."
  project      = var.project_id
}

# --- ingest -------------------------------------------------------------------

# Publisher on one topic. Not project-wide pubsub.publisher: that would let a
# compromised edge write to the dead-letter topic and forge evidence about what
# had failed.
resource "google_pubsub_topic_iam_member" "ingest_publisher" {
  topic   = google_pubsub_topic.raw.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.ingest.email}"
  project = var.project_id
}

# --- aggregator ---------------------------------------------------------------

resource "google_pubsub_subscription_iam_member" "aggregator_subscriber" {
  subscription = google_pubsub_subscription.aggregator.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.aggregator.email}"
  project      = var.project_id
}

resource "google_project_iam_member" "aggregator_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.aggregator.email}"
}

# --- query --------------------------------------------------------------------

resource "google_project_iam_member" "query_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.query.email}"
}

# --- shared -------------------------------------------------------------------

# Every service writes traces and metrics. These are grants to *write*
# telemetry, never to read it: a compromised service should not be able to
# enumerate what the rest of the fleet is doing.
resource "google_project_iam_member" "trace_agent" {
  for_each = {
    ingest     = google_service_account.ingest.email
    aggregator = google_service_account.aggregator.email
    query      = google_service_account.query.email
  }

  project = var.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "metric_writer" {
  for_each = {
    ingest     = google_service_account.ingest.email
    aggregator = google_service_account.aggregator.email
    query      = google_service_account.query.email
  }

  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "log_writer" {
  for_each = {
    ingest     = google_service_account.ingest.email
    aggregator = google_service_account.aggregator.email
    query      = google_service_account.query.email
  }

  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${each.value}"
}
