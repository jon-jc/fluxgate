# Database passwords remain sensitive Terraform state. Protect the remote state
# bucket with least-privilege access, versioning and audit logs.
resource "google_secret_manager_secret" "api_keys" {
  secret_id = "${local.name}-api-keys"
  project   = var.project_id
  labels    = local.common_labels
  replication {
    auto {}
  }
  depends_on = [google_project_service.required]
}
# Populate API_KEYS out of band. An empty placeholder must never become a revision.
resource "google_secret_manager_secret" "database_url" {
  secret_id = "${local.name}-database-url"
  project   = var.project_id
  labels    = local.common_labels
  replication {
    auto {}
  }
  depends_on = [google_project_service.required]
}
resource "google_secret_manager_secret_version" "database_url" {
  secret      = google_secret_manager_secret.database_url.id
  secret_data = format("postgres://%s:%s@localhost/%s?sslmode=disable", google_sql_user.app.name, urlencode(random_password.database.result), google_sql_database.fluxgate.name)
}
resource "google_secret_manager_secret_iam_member" "migrate_database_url" {
  secret_id = google_secret_manager_secret.database_url.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.migrate.email}"
  project   = var.project_id
}
resource "google_secret_manager_secret" "runtime_database_url" {
  for_each  = google_sql_user.runtime
  secret_id = "${local.name}-${each.key}-database-url"
  project   = var.project_id
  labels    = local.common_labels
  replication {
    auto {}
  }
}
resource "google_secret_manager_secret_version" "runtime_database_url" {
  for_each    = google_sql_user.runtime
  secret      = google_secret_manager_secret.runtime_database_url[each.key].id
  secret_data = format("postgres://%s:%s@localhost/%s?sslmode=disable", each.value.name, urlencode(random_password.runtime[each.key].result), google_sql_database.fluxgate.name)
}
resource "google_secret_manager_secret_iam_member" "runtime_database_url" {
  for_each  = local.runtime_accounts
  secret_id = google_secret_manager_secret.runtime_database_url[each.key].id
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${each.value}"
}
resource "google_secret_manager_secret_iam_member" "api_keys" {
  for_each  = { ingest = google_service_account.ingest.email, query = google_service_account.query.email }
  secret_id = google_secret_manager_secret.api_keys.id
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${each.value}"
}
