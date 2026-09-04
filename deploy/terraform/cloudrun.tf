# The three services.
#
# Cloud Run rather than GKE: there is no shared state between instances, no
# sidecar, and no need for a scheduler. A cluster would add an operational
# surface that buys nothing this workload uses.

locals {
  # Every service reports the same tier, project and collector.
  shared_env = {
    ENVIRONMENT    = var.environment
    GCP_PROJECT_ID = var.project_id
    LOG_FORMAT     = "json"
    LOG_LEVEL      = local.is_production ? "info" : "debug"

    PUBSUB_TOPIC_RAW               = google_pubsub_topic.raw.name
    PUBSUB_TOPIC_DLQ               = google_pubsub_topic.dead_letter.name
    PUBSUB_SUBSCRIPTION_AGGREGATOR = google_pubsub_subscription.aggregator.name

    # Deployed topology comes from this Terraform, never from a runtime admin
    # call. Configuration validation refuses it on production tiers anyway;
    # setting it explicitly makes the intent legible here too.
    PUBSUB_BOOTSTRAP = "false"

    # Cloud Run's built-in OpenTelemetry sidecar listens here.
    OTEL_EXPORTER_OTLP_ENDPOINT = "localhost:4317"
    TRACE_SAMPLE_RATIO          = local.is_production ? "0.05" : "1"
  }
}

resource "google_cloud_run_v2_service" "ingest" {
  name     = "${local.name}-ingest-api"
  location = var.region
  project  = var.project_id
  labels   = local.common_labels

  # The edge is public by design; the API key is the boundary.
  ingress = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.ingest.email

    scaling {
      min_instance_count = var.ingest_min_instances
      max_instance_count = var.ingest_max_instances
    }

    # A publish is I/O-bound, so one instance can serve many requests at once.
    # The number is bounded by the publisher's own outstanding-message limit,
    # not by CPU.
    max_instance_request_concurrency = 80

    containers {
      image = "${local.image_base}/ingest-api:${var.image_tag}"

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        # CPU only while a request is in flight. The edge does no background
        # work, so paying for always-allocated CPU would buy nothing.
        cpu_idle          = true
        startup_cpu_boost = true
      }

      ports {
        container_port = 8080
      }

      dynamic "env" {
        for_each = local.shared_env
        content {
          name  = env.key
          value = env.value
        }
      }

      env {
        name = "API_KEYS"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.api_keys.secret_id
            version = "latest"
          }
        }
      }

      # Behind Cloud Run's load balancer, X-Forwarded-For is rewritten and can
      # be trusted. It must stay false anywhere it is not.
      env {
        name  = "HTTP_TRUST_PROXY_HEADER"
        value = "true"
      }

      startup_probe {
        # Readiness, not liveness: the instance should not take traffic until
        # its publisher is usable.
        http_get {
          path = "/readyz"
        }
        initial_delay_seconds = 2
        period_seconds        = 3
        failure_threshold     = 10
      }

      liveness_probe {
        # Liveness asks only whether the process is wedged. Pointing it at
        # readiness would restart every instance during a broker outage, which
        # does not fix the broker and does lose every in-flight request.
        http_get {
          path = "/healthz"
        }
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

resource "google_cloud_run_v2_service" "aggregator" {
  name     = "${local.name}-aggregator"
  location = var.region
  project  = var.project_id
  labels   = local.common_labels

  # No public ingress. The aggregator serves probes and a scrape endpoint and
  # nothing else; exposing it would be surface for no purpose.
  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.aggregator.email

    scaling {
      # Fixed, not autoscaled: see the variable's comment. Scaling in mid-window
      # hands the survivors a redelivery of everything the departing instance
      # had not flushed.
      min_instance_count = var.aggregator_instances
      max_instance_count = var.aggregator_instances
    }

    vpc_access {
      connector = google_vpc_access_connector.main.id
      egress    = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = "${local.image_base}/aggregator:${var.image_tag}"

      resources {
        limits = {
          cpu = "1"
          # Windows are held in memory until they close, so the footprint
          # follows series cardinality rather than request rate.
          memory = "1Gi"
        }
        # The aggregator works between requests -- consuming, windowing,
        # flushing on a timer -- so its CPU must stay allocated. With cpu_idle
        # the flush timer would be throttled to a stop and windows would only
        # be written when a message happened to arrive.
        cpu_idle = false
      }

      ports {
        container_port = 8080
      }

      dynamic "env" {
        for_each = local.shared_env
        content {
          name  = env.key
          value = env.value
        }
      }

      env {
        name = "DATABASE_URL"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.database_url.secret_id
            version = "latest"
          }
        }
      }

      # The aggregator owns the schema. The query API deliberately does not, so
      # a read replica rolling out first cannot apply a change the writer is
      # not yet running.
      env {
        name  = "DATABASE_MIGRATE"
        value = "true"
      }

      startup_probe {
        http_get {
          path = "/readyz"
        }
        initial_delay_seconds = 5
        period_seconds        = 3
        failure_threshold     = 20
      }

      liveness_probe {
        http_get {
          path = "/healthz"
        }
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

resource "google_cloud_run_v2_service" "query" {
  name     = "${local.name}-query-api"
  location = var.region
  project  = var.project_id
  labels   = local.common_labels

  ingress = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.query.email

    scaling {
      min_instance_count = var.query_min_instances
      max_instance_count = var.query_max_instances
    }

    # Lower than the edge's: a query holds a database connection for its
    # duration, and the pool is the scarce resource here rather than CPU.
    max_instance_request_concurrency = 40

    # The live tail holds a connection open for minutes at a time, so the
    # request timeout has to exceed the stream's own bound.
    timeout = "3600s"

    vpc_access {
      connector = google_vpc_access_connector.main.id
      egress    = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = "${local.image_base}/query-api:${var.image_tag}"

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        # A live tail polls between requests, so CPU stays allocated for the
        # same reason as the aggregator.
        cpu_idle          = false
        startup_cpu_boost = true
      }

      ports {
        container_port = 8080
      }

      dynamic "env" {
        for_each = local.shared_env
        content {
          name  = env.key
          value = env.value
        }
      }

      env {
        name = "API_KEYS"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.api_keys.secret_id
            version = "latest"
          }
        }
      }

      env {
        name = "DATABASE_URL"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.database_url.secret_id
            version = "latest"
          }
        }
      }

      # Read-only: schema changes belong to the writer.
      env {
        name  = "DATABASE_MIGRATE"
        value = "false"
      }

      env {
        name  = "HTTP_TRUST_PROXY_HEADER"
        value = "true"
      }

      startup_probe {
        http_get {
          path = "/readyz"
        }
        initial_delay_seconds = 2
        period_seconds        = 3
        failure_threshold     = 10
      }

      liveness_probe {
        http_get {
          path = "/healthz"
        }
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
