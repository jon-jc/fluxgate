# Service identities, one per service, each granted only what that service
# actually does.
#
# The temptation is one account with roles/editor and a note to tighten it
# later. The reason not to is concrete: the ingest API accepts arbitrary input
# from the public internet, and if it is ever compromised the blast radius is
# exactly the permissions it holds. It publishes to one topic. That is all it
# should be able to do.

resource "google_service_account" "ingest" {
  depends_on   = [google_project_service.required]
  account_id   = "${local.name}-ingest"
  display_name = "Fluxgate ingest API (${var.environment})"
  description  = "Publishes validated batches. Reserves retry identities and publishes; cannot read rollups."
  project      = var.project_id
}

resource "google_service_account" "aggregator" {
  depends_on   = [google_project_service.required]
  account_id   = "${local.name}-aggregator"
  display_name = "Fluxgate aggregator (${var.environment})"
  description  = "Consumes batches and writes rollups. No publish rights."
  project      = var.project_id
}

resource "google_service_account" "query" {
  depends_on   = [google_project_service.required]
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


resource "google_service_account" "migrate" {
  project      = var.project_id
  account_id   = "${local.name}-migrate"
  display_name = "Fluxgate schema migration owner"
  depends_on   = [google_project_service.required]
}
resource "google_project_iam_member" "ingest_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.ingest.email}"
}
resource "google_project_iam_member" "migrate_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.migrate.email}"
}
resource "google_cloud_run_v2_service_iam_member" "public_api" {
  for_each = var.deploy_services ? toset(["ingest", "query"]) : toset([])
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.service[each.key].name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
