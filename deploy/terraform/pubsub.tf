# The event topology.
#
# This is the deployed counterpart of the bootstrap the services run against the
# emulator. Deployed topology is created here rather than at runtime because a
# service that can create its own infrastructure needs admin permissions on the
# request path, which is a far larger blast radius than publish-and-subscribe.

resource "google_pubsub_topic" "raw" {
  name    = "${local.name}-telemetry-raw"
  labels  = local.common_labels
  project = var.project_id

  message_retention_duration = var.message_retention

  # Cloud Storage would be a cheaper archive, but retaining on the topic means a
  # subscription created after an incident can replay what it missed.
}

resource "google_pubsub_topic" "dead_letter" {
  name    = "${local.name}-telemetry-dlq"
  labels  = local.common_labels
  project = var.project_id

  # Poisoned messages are kept far longer than live ones. They are, by
  # definition, the ones somebody will want to look at -- and they will not
  # look at them today.
  message_retention_duration = "604800s" # 7 days
}

resource "google_pubsub_subscription" "aggregator" {
  name    = "${local.name}-telemetry-aggregator"
  topic   = google_pubsub_topic.raw.id
  labels  = local.common_labels
  project = var.project_id

  ack_deadline_seconds       = var.ack_deadline_seconds
  message_retention_duration = var.message_retention
  # An acknowledged message is gone. Retaining them would mean a redeployed
  # subscription silently reprocesses history the ledger would then suppress,
  # burning quota to reach the same state.
  retain_acked_messages = false

  # Without an explicit retry policy a nacked message returns immediately. A
  # consumer failing because its database is down would then hammer that
  # database as fast as it can fail, turning a brief outage into a sustained
  # one.
  retry_policy {
    minimum_backoff = "1s"
    maximum_backoff = "60s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = var.max_delivery_attempts
  }

  expiration_policy {
    # Never expire. The default deletes a subscription after 31 days of
    # inactivity, which for a pipeline that goes quiet over a holiday would
    # silently discard every message published after it vanished.
    ttl = ""
  }

  depends_on = [
    google_pubsub_subscription_iam_member.dead_letter_subscriber,
    google_pubsub_topic_iam_member.dead_letter_publisher,
  ]
}

resource "google_pubsub_subscription" "dead_letter_inspect" {
  name    = "${local.name}-telemetry-dlq-inspect"
  topic   = google_pubsub_topic.dead_letter.id
  labels  = local.common_labels
  project = var.project_id

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"

  # A dead-letter topic with no subscription is a hole messages fall into. This
  # one exists so they can be inspected and replayed rather than ageing out
  # unexamined.
  expiration_policy {
    ttl = ""
  }
}

# --- dead-letter permissions --------------------------------------------------
#
# Dead lettering is performed by Pub/Sub's own service agent, not by the
# consumer, and it silently does nothing unless that agent can publish to the
# dead-letter topic and acknowledge on the source subscription. Getting this
# wrong produces no error at apply time and no error at run time: messages
# simply keep being redelivered forever. It is the single most common way a
# dead-letter policy is configured and never actually fires.

data "google_project" "current" {
  project_id = var.project_id
}

locals {
  pubsub_service_agent = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "dead_letter_publisher" {
  topic   = google_pubsub_topic.dead_letter.name
  role    = "roles/pubsub.publisher"
  member  = local.pubsub_service_agent
  project = var.project_id
}

resource "google_pubsub_subscription_iam_member" "dead_letter_subscriber" {
  subscription = "${local.name}-telemetry-aggregator"
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_service_agent
  project      = var.project_id

  # Named rather than referenced, because the subscription depends on this
  # binding: referencing it would be a cycle.
  depends_on = [google_pubsub_topic.raw]
}
