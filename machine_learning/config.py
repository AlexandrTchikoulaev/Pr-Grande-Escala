import os

MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT",      "localhost:9004")
MINIO_ACCESS        = os.getenv("MINIO_ACCESS",        "minioadmin")
MINIO_SECRET        = os.getenv("MINIO_SECRET",        "minioadmin")
HMS_URI             = os.getenv("HMS_URI",              "thrift://localhost:9083")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")

GOLD_BUCKET          = "gold"
CHURN_DAYS_THRESHOLD = 30
