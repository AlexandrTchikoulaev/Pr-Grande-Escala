import os

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP       = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
CLICKSTREAM_TOPIC     = "clickstream_events"

# ---------------------------------------------------------------------------
# PostgreSQL  (Olist operational DB)
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5434"))
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_NAME     = os.getenv("DB_NAME", "olist_db")

# ---------------------------------------------------------------------------
# MinIO  (raw reviews bucket)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "localhost:9004")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET",   "minioadmin")
REVIEWS_BUCKET  = "raw-reviews"

# ---------------------------------------------------------------------------
# Olist reference CSVs
# ---------------------------------------------------------------------------
OLIST_DATA_PATH = os.getenv(
    "OLIST_DATA_PATH",
    r"c:\Users\Alexandr\Desktop\Universidade\3º Ano\Grande Escala\Projeto Versão Final\Projeto-Grande-Escala\Codes\Prepare Data\Amazon\Data"
)

# ---------------------------------------------------------------------------
# Simulation parameters
# ---------------------------------------------------------------------------
SESSIONS_PER_SECOND = float(os.getenv("SESSIONS_PER_SECOND", "0.5"))

DEVICES         = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS  = [0.55, 0.35, 0.10]

# Funnel transition probabilities
P_SEARCH            = 0.60   # session_start → search (else category_browse)
P_PRODUCT_VIEW      = 0.70   # after navigation → product_view (else bounce)
P_REVIEW_READ       = 0.40   # after product_view → product_review_read
P_ADD_TO_CART       = 0.35   # after product_view → add_to_cart
P_CART_ABANDON      = 0.45   # after cart_view → cart_abandon (else checkout)
P_REMOVE_FROM_CART  = 0.15   # after add_to_cart → remove_from_cart first
P_ORDER_PLACED      = 0.80   # after checkout_start → order_placed
P_REVIEW_SUBMIT     = 0.40   # after order_placed → review_submitted (delayed)

# Delay between events within a session (seconds)
EVENT_DELAY_MIN = 2
EVENT_DELAY_MAX = 15

# Delay before a post-purchase review is submitted (seconds, simulated)
REVIEW_DELAY_MIN = 30
REVIEW_DELAY_MAX = 300
