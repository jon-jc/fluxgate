# Secrets live in Secret Manager and are mounted, never baked into an image or
# passed as a plain environment variable in the service definition.
#
# The difference matters: a plain env var in a Cloud Run revision is visible to
# anyone with run.services.get, which is a much wider set of people than those
# who should be able to read a credential.

resource "google_secret_manager_secret" "api_keys" {
  secret_id = "${local.name}-api-keys"
  project   = var.project_id
  labels    = local.common_labels

  replication {
    auto {}
  }
}

# The value is set out of band, not by Terraform.
#
# Putting the key document in a variable would put it in the state file, which
# is stored in GCS and read by anyone who can run a plan. The secret is created
# empty here and populated with:
#
#   gcloud secrets versions add fluxgate-prod-api-keys --data-file=keys.json
#
# where keys.json holds the SHA-256 digests, never the plaintext secrets.
resource "google_secret_manager_secret_version" "api_keys_placeholder" {
  secret      = google_secret_manager_secret.api_keys.id
  secret_data = "[]"

  lifecycle {
    # Terraform creates the first version so the services have something to
    # mount, then never touches it again. Managing versions here would mean
    # every apply reverts whatever was rotated in by hand.
    ignore_changes = [secret_data]
  }
}

resource "google_secret_manager_secret" "database_url" {
  secret_id = "${local.name}-database-url"
  project   = var.project_id
  labels    = local.common_labels

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "database_url" {
  secret = google_secret_manager_secret.database_url.id

  # The password never appears in a service definition or a log line. It is
  # still in Terraform state, which is why that state lives in a GCS bucket
  # with restricted access rather than on anyone's laptop.
  secret_data = format(
    "postgres://%s:%s@%s/%s?sslmode=require",
    google_sql_user.app.name,
    urlencode(random_password.database.result),
    google_sql_database_instance.main.private_ip_address,
    google_sql_database.fluxgate.name,
  )
}

# --- who may read what --------------------------------------------------------
#
# Granted per secret and per service, not project-wide. The ingest API has no
# reason to read a database URL, and the aggregator has no reason to read API
# keys: it authenticates nobody.

resource "google_secret_manager_secret_iam_member" "ingest_api_keys" {
  secret_id = google_secret_manager_secret.api_keys.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.ingest.email}"
  project   = var.project_id
}

resource "google_secret_manager_secret_iam_member" "query_api_keys" {
  secret_id = google_secret_manager_secret.api_keys.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.query.email}"
  project   = var.project_id
}

resource "google_secret_manager_secret_iam_member" "aggregator_database_url" {
  secret_id = google_secret_manager_secret.database_url.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.aggregator.email}"
  project   = var.project_id
}

resource "google_secret_manager_secret_iam_member" "query_database_url" {
  secret_id = google_secret_manager_secret.database_url.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.query.email}"
  project   = var.project_id
}
