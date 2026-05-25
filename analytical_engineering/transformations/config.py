import os

# ---------------------------------------------------------------------------
# MinIO (S3A)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9004")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",   "minioadmin")

SILVER_BUCKET  = "silver"
GOLD_BUCKET    = "gold"

# ---------------------------------------------------------------------------
# Hive Metastore (Iceberg catalog)
# ---------------------------------------------------------------------------
HMS_URI = os.getenv("HMS_URI", "thrift://localhost:9083")

# ---------------------------------------------------------------------------
# Trino (for views)
# ---------------------------------------------------------------------------
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))

# ---------------------------------------------------------------------------
# Dimension date range
# ---------------------------------------------------------------------------
DIM_DATE_START = "2020-01-01"
DIM_DATE_END   = "2030-12-31"

# ---------------------------------------------------------------------------
# Sentiment thresholds (based on rating 1–5)
# ---------------------------------------------------------------------------
SENTIMENT_POSITIVE = 4   # rating >= 4
SENTIMENT_NEUTRAL  = 3   # rating == 3
# rating <= 2 → negative

# ---------------------------------------------------------------------------
# Brazil state metadata
# ---------------------------------------------------------------------------
REGION_MAP = {
    "SP": ("São Paulo",             "Sudeste"),
    "RJ": ("Rio de Janeiro",        "Sudeste"),
    "MG": ("Minas Gerais",          "Sudeste"),
    "ES": ("Espírito Santo",        "Sudeste"),
    "RS": ("Rio Grande do Sul",     "Sul"),
    "PR": ("Paraná",                "Sul"),
    "SC": ("Santa Catarina",        "Sul"),
    "BA": ("Bahia",                 "Nordeste"),
    "PE": ("Pernambuco",            "Nordeste"),
    "CE": ("Ceará",                 "Nordeste"),
    "MA": ("Maranhão",              "Nordeste"),
    "PB": ("Paraíba",               "Nordeste"),
    "RN": ("Rio Grande do Norte",   "Nordeste"),
    "AL": ("Alagoas",               "Nordeste"),
    "SE": ("Sergipe",               "Nordeste"),
    "PI": ("Piauí",                 "Nordeste"),
    "PA": ("Pará",                  "Norte"),
    "AM": ("Amazonas",              "Norte"),
    "RO": ("Rondônia",              "Norte"),
    "AC": ("Acre",                  "Norte"),
    "AP": ("Amapá",                 "Norte"),
    "RR": ("Roraima",               "Norte"),
    "TO": ("Tocantins",             "Norte"),
    "DF": ("Distrito Federal",      "Centro-Oeste"),
    "GO": ("Goiás",                 "Centro-Oeste"),
    "MT": ("Mato Grosso",           "Centro-Oeste"),
    "MS": ("Mato Grosso do Sul",    "Centro-Oeste"),
}
