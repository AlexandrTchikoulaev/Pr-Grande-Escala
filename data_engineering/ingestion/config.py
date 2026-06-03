import os

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP            = os.getenv("KAFKA_BOOTSTRAP",  "localhost:29092")
CLICKSTREAM_TOPIC          = "clickstream_events"
CDC_TOPICS                 = ["debezium.public.orders", "debezium.public.order_items"]
GROUP_CLICKSTREAM          = "de_clickstream_consumer"
GROUP_CDC                  = "de_cdc_consumer"

# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "localhost:9004")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET",   "minioadmin")

BRONZE_BUCKET   = "bronze"
REVIEWS_BUCKET  = "raw-reviews"

# ---------------------------------------------------------------------------
# Buffer / flush settings
# ---------------------------------------------------------------------------
BUFFER_SIZE        = 500    # flush after N records
FLUSH_INTERVAL     = 30     # flush after N seconds (whichever comes first)

# ---------------------------------------------------------------------------
# File watcher
# ---------------------------------------------------------------------------
WATCHER_INTERVAL   = 30     # seconds between MinIO polls
WATCHER_STATE_FILE = "ingestion/.watcher_state.json"  # tracks processed files
