# Cloud SQL for Postgres, reachable only over private IP.
#
# A public IP with an authorised-network allowlist is the common shortcut. It is
# also how a database ends up exposed the first time somebody adds 0.0.0.0/0 to
# debug a connection at 2am and forgets to remove it.

resource "google_compute_network" "main" {
  name                    = "${local.name}-network"
  auto_create_subnetworks = false
  project                 = var.project_id
}

resource "google_compute_subnetwork" "main" {
  name          = "${local.name}-subnet"
  ip_cidr_range = "10.20.0.0/24"
  region        = var.region
  network       = google_compute_network.main.id
  project       = var.project_id

  # Flow logs cost money and answer "who talked to the database" during an
  # incident, which is a question with no other source of truth.
  log_config {
    aggregation_interval = "INTERVAL_10_MIN"
    flow_sampling        = 0.5
    metadata             = "INCLUDE_ALL_METADATA"
  }
}

# Cloud SQL's private IP comes from a range peered into the VPC.
resource "google_compute_global_address" "private_ip" {
  name          = "${local.name}-sql-private-ip"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.main.id
  project       = var.project_id
}

resource "google_service_networking_connection" "private_vpc" {
  network                 = google_compute_network.main.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip.name]
}

resource "random_password" "database" {
  length  = 32
  special = true
  # Excluded because a DSN is a URL: these characters would have to be
  # percent-encoded, and the encoding is exactly the step somebody forgets.
  override_special = "-_.~"
}

resource "google_sql_database_instance" "main" {
  name             = "${local.name}-postgres"
  database_version = "POSTGRES_17"
  region           = var.region
  project          = var.project_id
  # Deletion protection on every tier. A destroyed database is not recoverable
  # from Terraform state, and the cost of typing an extra flag is nothing next
  # to the cost of finding out.
  deletion_protection = true

  depends_on = [google_service_networking_connection.private_vpc]

  settings {
    tier              = var.database_tier
    availability_type = var.database_availability_type
    disk_type         = "PD_SSD"
    disk_size         = var.database_disk_gb
    # Growing on demand beats a 3am page about a full disk; it does not shrink,
    # so the floor above still matters.
    disk_autoresize       = true
    disk_autoresize_limit = var.database_disk_gb * 4

    user_labels = local.common_labels

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.main.id
      # Even on the private network: the aggregator and the query API both
      # connect through the Cloud SQL proxy, which authenticates by IAM.
      ssl_mode = "ENCRYPTED_ONLY"
    }

    backup_configuration {
      enabled                        = true
      start_time                     = "03:00"
      point_in_time_recovery_enabled = local.is_production
      transaction_log_retention_days = local.is_production ? 7 : 1

      backup_retention_settings {
        retained_backups = local.is_production ? 30 : 7
        retention_unit   = "COUNT"
      }
    }

    maintenance_window {
      day  = 7 # Sunday
      hour = 4
      # Stable rather than preview: this database is the only place rollups
      # exist, and being early to a Postgres patch buys nothing here.
      update_track = "stable"
    }

    insights_config {
      query_insights_enabled  = true
      record_application_tags = true
      # Client addresses are not recorded: on a private network every
      # connection comes from the proxy, so the field is noise with a privacy
      # cost attached.
      record_client_address = false
    }

    database_flags {
      # Log anything slower than a second. The flush transaction and the query
      # path are both well under that, so this stays quiet until it matters.
      name  = "log_min_duration_statement"
      value = "1000"
    }
  }

  lifecycle {
    # The tier is changed deliberately during a resize, and a provider default
    # drifting underneath would otherwise propose a restart on an unrelated
    # apply.
    ignore_changes = [settings[0].disk_size]
  }
}

resource "google_sql_database" "fluxgate" {
  name     = "fluxgate"
  instance = google_sql_database_instance.main.name
  project  = var.project_id
}

resource "google_sql_user" "app" {
  name     = "fluxgate"
  instance = google_sql_database_instance.main.name
  password = random_password.database.result
  project  = var.project_id
}

# Cloud Run reaches the private database through a direct VPC egress connector.
resource "google_vpc_access_connector" "main" {
  name          = "${local.name}-connector"
  region        = var.region
  network       = google_compute_network.main.name
  ip_cidr_range = "10.21.0.0/28"
  project       = var.project_id

  min_instances = 2
  max_instances = 3
}
