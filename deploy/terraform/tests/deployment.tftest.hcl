mock_provider "google" {}
mock_provider "google-beta" {}
mock_provider "random" {}

variables {
  project_id  = "fluxgate-test"
  environment = "dev"
}

run "bootstrap_has_no_services" {
  command = plan
  assert {
    condition     = length(google_cloud_run_v2_service.service) == 0 && length(google_cloud_run_v2_job.migrate) == 0
    error_message = "First apply must not attempt to start unconfigured services or migrations."
  }
  assert {
    condition     = length(google_sql_user.runtime) == 3
    error_message = "Every runtime requires a distinct database identity."
  }
}

run "reject_mutable_images" {
  command = plan
  variables {
    images = { ingest = "example/ingest:latest", aggregator = "", query = "", migrate = "" }
  }
  expect_failures = [var.images]
}

run "reject_unconfigured_services" {
  command = plan
  variables { deploy_services = true }
  expect_failures = [terraform_data.deployment_gate]
}

run "reject_oversubscribed_database" {
  command = plan
  variables { database_max_connections = 30 }
  expect_failures = [terraform_data.deployment_gate]
}

run "production_requires_operations" {
  command = plan
  variables {
    environment      = "prod"
    deploy_services  = true
    api_keys_version = "1"
    images = {
      ingest     = "example/ingest@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      aggregator = "example/aggregator@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      query      = "example/query@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      migrate    = "example/migrate@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  }
  expect_failures = [terraform_data.deployment_gate]
}

run "configured_production" {
  command = plan
  variables {
    environment                 = "prod"
    deploy_services             = true
    api_keys_version            = "2"
    database_availability_type  = "REGIONAL"
    alert_notification_channels = ["projects/fluxgate-test/notificationChannels/123"]
    images = {
      ingest     = "example/ingest@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      aggregator = "example/aggregator@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      query      = "example/query@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      migrate    = "example/migrate@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  }
  assert {
    condition     = length(google_cloud_run_v2_service.service) == 3 && length(google_cloud_run_v2_service_iam_member.public_api) == 2
    error_message = "Only ingest and query may be public; all three services must exist."
  }
  assert {
    condition     = local.shared_env.DATABASE_MIGRATE == "false" && local.shared_env.OTEL_EXPORTER_OTLP_ENDPOINT == "" && local.shared_env.LEDGER_RETENTION == "864000s"
    error_message = "Runtime migration, collector, and replay-retention safeguards must hold."
  }
}
