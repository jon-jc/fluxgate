# Alert policies.
#
# Each one exists because it catches a failure the others miss, and each pages
# on a symptom a user would notice rather than on a cause that may be harmless.
# An alert nobody acts on trains people to ignore the ones that matter.

resource "google_monitoring_alert_policy" "dead_letter_backlog" {
  display_name = "${local.name}: messages are being dead-lettered"
  project      = var.project_id
  combiner     = "OR"

  documentation {
    content   = <<-EOT
      Messages are arriving on the dead-letter topic, which means the aggregator
      rejected them ${var.max_delivery_attempts} times.

      This is almost always a poisoned payload -- an envelope the consumer
      cannot decode, or a schema version this build does not understand. It is
      never transient: a message reaches the dead-letter queue only after the
      retry policy has already given it several chances.

      Pull one from ${google_pubsub_subscription.dead_letter_inspect.name} and
      read it. The payload is JSON precisely so this step needs no tooling.
    EOT
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Dead-letter topic receiving messages"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"pubsub_topic\"",
        "resource.labels.topic_id = \"${google_pubsub_topic.dead_letter.name}\"",
        "metric.type = \"pubsub.googleapis.com/topic/send_message_operation_count\"",
      ])

      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "300s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  notification_channels = var.alert_notification_channels

  alert_strategy {
    auto_close = "3600s"
  }
}

resource "google_monitoring_alert_policy" "subscription_backlog" {
  display_name = "${local.name}: the aggregator is falling behind"
  project      = var.project_id
  combiner     = "OR"

  documentation {
    content   = <<-EOT
      The oldest unacknowledged message on the aggregator subscription is older
      than the acknowledgement deadline allows for, which means consumption is
      slower than production.

      Check, in order: the flush duration (a slow database is the usual cause),
      the tracked-series gauge (a cardinality spike makes every window more
      expensive), and the instance count.

      Note that the aggregator holds messages deliberately -- it acknowledges
      only once a window is durable -- so a backlog roughly the size of one
      window is normal and expected. This threshold is set well above that.
    EOT
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Oldest unacknowledged message age"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"pubsub_subscription\"",
        "resource.labels.subscription_id = \"${google_pubsub_subscription.aggregator.name}\"",
        "metric.type = \"pubsub.googleapis.com/subscription/oldest_unacked_message_age\"",
      ])

      comparison = "COMPARISON_GT"
      # Ten minutes: comfortably above one window plus its lateness allowance
      # plus a flush interval, so ordinary operation never trips it.
      threshold_value = 600
      duration        = "300s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  notification_channels = var.alert_notification_channels
}

resource "google_monitoring_alert_policy" "ingest_errors" {
  display_name = "${local.name}: the ingest API is returning server errors"
  project      = var.project_id
  combiner     = "OR"

  documentation {
    content   = <<-EOT
      The ingest API is returning 5xx responses.

      Only server faults are covered here. A 4xx means a client sent something
      invalid and the API told them so, which is the system working; alerting on
      it would page somebody for another team's bug.

      A 503 specifically means the edge is shedding load or the publish circuit
      breaker is open -- check whether Pub/Sub is healthy before looking at the
      service itself.
    EOT
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "5xx rate on the ingest service"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_revision\"",
        "resource.labels.service_name = \"${google_cloud_run_v2_service.ingest.name}\"",
        "metric.type = \"run.googleapis.com/request_count\"",
        "metric.labels.response_code_class = \"5xx\"",
      ])

      comparison = "COMPARISON_GT"
      # A steady trickle rather than a single blip: one 5xx during a deploy is
      # not worth waking anyone.
      threshold_value = 1
      duration        = "300s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  notification_channels = var.alert_notification_channels
}

resource "google_monitoring_alert_policy" "database_disk" {
  display_name = "${local.name}: the database disk is filling"
  project      = var.project_id
  combiner     = "OR"

  documentation {
    content   = <<-EOT
      Cloud SQL disk utilisation is high.

      Autoresize is enabled, so this is a warning rather than an outage: it
      means growth is outpacing what retention is reclaiming. Check whether the
      retention job is running, and whether rollup cardinality has changed.

      A full disk on this instance stops the aggregator committing, which stops
      it acknowledging, which turns into a subscription backlog and eventually
      dead-lettered messages. Catching it here is much cheaper.
    EOT
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Disk utilisation above 80%"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloudsql_database\"",
        "resource.labels.database_id = \"${var.project_id}:${google_sql_database_instance.main.name}\"",
        "metric.type = \"cloudsql.googleapis.com/database/disk/utilization\"",
      ])

      comparison      = "COMPARISON_GT"
      threshold_value = 0.8
      duration        = "600s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  notification_channels = var.alert_notification_channels
}
