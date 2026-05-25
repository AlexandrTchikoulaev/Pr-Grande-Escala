import os

# ---------------------------------------------------------------------------
# MinIO (S3A)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "localhost:9004")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET",   "minioadmin")

BRONZE_BUCKET   = "bronze"
SILVER_BUCKET   = "silver"

# ---------------------------------------------------------------------------
# Hive Metastore (Iceberg catalog)
# ---------------------------------------------------------------------------
HMS_URI = os.getenv("HMS_URI", "thrift://localhost:9083")

# ---------------------------------------------------------------------------
# Spark Structured Streaming trigger interval
# ---------------------------------------------------------------------------
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "60 seconds")

# ---------------------------------------------------------------------------
# Checkpoint locations (inside silver bucket)
# ---------------------------------------------------------------------------
CHECKPOINT_CLICKSTREAM = f"s3a://{SILVER_BUCKET}/_checkpoints/clickstream"
CHECKPOINT_ORDERS      = f"s3a://{SILVER_BUCKET}/_checkpoints/orders"
CHECKPOINT_REVIEWS     = f"s3a://{SILVER_BUCKET}/_checkpoints/reviews"

# ---------------------------------------------------------------------------
# Brazil state → region mapping
# ---------------------------------------------------------------------------
REGION_MAP = {
    "SP": "Sudeste", "RJ": "Sudeste", "MG": "Sudeste", "ES": "Sudeste",
    "RS": "Sul",     "PR": "Sul",     "SC": "Sul",
    "BA": "Nordeste","PE": "Nordeste","CE": "Nordeste","MA": "Nordeste",
    "PB": "Nordeste","RN": "Nordeste","AL": "Nordeste","SE": "Nordeste","PI": "Nordeste",
    "PA": "Norte",   "AM": "Norte",   "RO": "Norte",   "AC": "Norte",
    "AP": "Norte",   "RR": "Norte",   "TO": "Norte",
    "DF": "Centro-Oeste","GO": "Centro-Oeste","MT": "Centro-Oeste","MS": "Centro-Oeste",
}
