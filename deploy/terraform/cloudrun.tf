locals {
  runtime_accounts = {
    ingest     = google_service_account.ingest.email
    aggregator = google_service_account.aggregator.email
    query      = google_service_account.query.email
  }
  services = {
    ingest     = { suffix = "ingest-api", min = var.ingest_min_instances, max = var.ingest_max_instances, concurrency = 80 }
    aggregator = { suffix = "aggregator", min = var.aggregator_instances, max = var.aggregator_instances, concurrency = 1 }
    query      = { suffix = "query-api", min = var.query_min_instances, max = var.query_max_instances, concurrency = 40 }
  }
  shared_env = {
    ENVIRONMENT                    = var.environment
    GCP_PROJECT_ID                 = var.project_id
    LOG_FORMAT                     = "json"
    LOG_LEVEL                      = "info"
    PUBSUB_TOPIC_RAW               = google_pubsub_topic.raw.name
    PUBSUB_TOPIC_DLQ               = google_pubsub_topic.dead_letter.name
    PUBSUB_SUBSCRIPTION_AGGREGATOR = google_pubsub_subscription.aggregator.name
    PUBSUB_BOOTSTRAP               = "false"
    PUBSUB_RETENTION               = var.message_retention
    PUBSUB_DLQ_RETENTION           = "604800s"
    # Include a client's retry horizon before a fresh publish and a safety day.
    LEDGER_RETENTION       = "${try(tonumber(trimsuffix(var.message_retention, "s")), 86400) + 604800 + 172800}s"
    CLOUD_SQL_INSTANCE     = google_sql_database_instance.main.connection_name
    DATABASE_MIGRATE       = "false"
    DATABASE_MAX_CONNS     = tostring(var.database_pool_connections)
    DATABASE_MIN_CONNS     = "0"
    SHUTDOWN_GRACE_PERIOD  = "0s"
    SHUTDOWN_DRAIN_TIMEOUT = "5s"
    PUBSUB_PUBLISH_TIMEOUT = "4s"
    HTTP_HANDLER_TIMEOUT   = "6s"
    HTTP_WRITE_TIMEOUT     = "10s"
    # Platform metrics work without a collector. Optional tracing requires one.
    OTEL_EXPORTER_OTLP_ENDPOINT = var.otlp_endpoint
    OTEL_EXPORTER_OTLP_INSECURE = "false"
    TRACE_SAMPLE_RATIO          = "0.05"
  }
}

resource "google_cloud_run_v2_service" "service" {
  for_each = var.deploy_services ? local.services : {}
  project  = var.project_id
  name     = "${local.name}-${each.value.suffix}"
  location = var.region
  labels   = local.common_labels
  ingress  = each.key == "aggregator" ? "INGRESS_TRAFFIC_INTERNAL_ONLY" : "INGRESS_TRAFFIC_ALL"
  depends_on = [
    terraform_data.deployment_gate, google_project_service.required,
    google_secret_manager_secret_iam_member.runtime_database_url,
    google_secret_manager_secret_iam_member.api_keys,
    google_project_iam_member.ingest_sql_client, google_project_iam_member.aggregator_sql_client,
    google_project_iam_member.query_sql_client,
    google_pubsub_subscription_iam_member.aggregator_subscriber,
    google_pubsub_subscription_iam_member.dead_letter_subscriber,
    google_pubsub_topic_iam_member.ingest_publisher,
  ]
  template {
    service_account = local.runtime_accounts[each.key]
    scaling {
      min_instance_count = each.value.min
      max_instance_count = each.value.max
    }
    max_instance_request_concurrency = each.value.concurrency
    timeout                          = each.key == "query" ? "3600s" : "30s"
    vpc_access {
      connector = google_vpc_access_connector.main.id
      egress    = "PRIVATE_RANGES_ONLY"
    }
    containers {
      image = var.images[each.key]
      resources {
        limits            = { cpu = "1", memory = each.key == "aggregator" ? "1Gi" : "512Mi" }
        cpu_idle          = each.key == "ingest"
        startup_cpu_boost = true
      }
      ports { container_port = 8080 }
      dynamic "env" {
        for_each = merge(local.shared_env, {
          METRICS_ENABLED = each.key == "aggregator" ? "true" : "false"
          GOMEMLIMIT      = each.key == "aggregator" ? "700MiB" : "350MiB"
        })
        content {
          name  = env.key
          value = env.value
        }
      }
      env {
        name = "DATABASE_URL"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.runtime_database_url[each.key].secret_id
            version = google_secret_manager_secret_version.runtime_database_url[each.key].version
          }
        }
      }
      dynamic "env" {
        for_each = each.key == "aggregator" ? [] : [1]
        content {
          name = "API_KEYS"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.api_keys.secret_id
              version = var.api_keys_version
            }
          }
        }
      }
      startup_probe {
        http_get { path = "/readyz" }
        initial_delay_seconds = 5
        period_seconds        = 3
        failure_threshold     = 20
      }
      liveness_probe {
        http_get { path = "/healthz" }
        period_seconds    = 30
        failure_threshold = 3
      }
    }
  }
  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
}

# Terraform creates the job but never executes it. The operator must review and
# run it, then enable the services. A separate identity owns schema changes.
resource "google_cloud_run_v2_job" "migrate" {
  count      = var.images.migrate == "" ? 0 : 1
  project    = var.project_id
  name       = "${local.name}-migrate"
  location   = var.region
  labels     = local.common_labels
  depends_on = [google_secret_manager_secret_iam_member.migrate_database_url, google_project_iam_member.migrate_sql_client, google_sql_user.runtime]
  template {
    template {
      service_account = google_service_account.migrate.email
      timeout         = "600s"
      max_retries     = 0
      vpc_access {
        connector = google_vpc_access_connector.main.id
        egress    = "PRIVATE_RANGES_ONLY"
      }
      containers {
        image = var.images.migrate
        args  = ["-grant-runtime-roles"]
        env {
          name  = "ENVIRONMENT"
          value = var.environment
        }
        env {
          name  = "CLOUD_SQL_INSTANCE"
          value = google_sql_database_instance.main.connection_name
        }
        env {
          name = "DATABASE_URL"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.database_url.secret_id
              version = google_secret_manager_secret_version.database_url.version
            }
          }
        }
      }
    }
  }
}
