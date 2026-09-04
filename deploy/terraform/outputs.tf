output "ingest_url" {
  description = "Public URL of the ingest API."
  value       = google_cloud_run_v2_service.ingest.uri
}

output "query_url" {
  description = "Public URL of the query API."
  value       = google_cloud_run_v2_service.query.uri
}

output "raw_topic" {
  description = "Topic carrying accepted batches."
  value       = google_pubsub_topic.raw.name
}

output "dead_letter_topic" {
  description = "Topic holding messages that exhausted their delivery attempts."
  value       = google_pubsub_topic.dead_letter.name
}

output "dead_letter_subscription" {
  description = "Pull from this to inspect a poisoned message."
  value       = google_pubsub_subscription.dead_letter_inspect.name
}

output "database_instance" {
  description = "Cloud SQL instance name."
  value       = google_sql_database_instance.main.name
}

output "database_private_ip" {
  description = "Private IP of the database. Not reachable from outside the VPC."
  value       = google_sql_database_instance.main.private_ip_address
}

output "service_accounts" {
  description = "Per-service identities, each holding only what that service does."
  value = {
    ingest     = google_service_account.ingest.email
    aggregator = google_service_account.aggregator.email
    query      = google_service_account.query.email
  }
}

output "api_keys_secret" {
  description = <<-EOT
    Secret holding the API key document.

    Populate it out of band, so the plaintext never enters Terraform state:
      gcloud secrets versions add <this> --data-file=keys.json
  EOT
  value       = google_secret_manager_secret.api_keys.secret_id
}
