"""
MinIO writer — stores reviews as raw .txt files.
Unstructured data source: free-text reviews with no structured header.
Metadata (review_id, order_id) is carried in the filename only.

File path convention:
  raw-reviews/{YYYY-MM-DD}/{review_id}_{order_id}.txt
"""

import io
import random
import boto3
from botocore.exceptions import ClientError
from simulator import config

_FIRST_NAMES = [
    "Ana", "João", "Maria", "Pedro", "Sofia", "Carlos", "Inês", "Miguel",
    "Beatriz", "Rui", "Mariana", "Tiago", "Catarina", "Luís", "Filipa",
    "André", "Rita", "Nuno", "Cláudia", "Diogo", "Vera", "Bruno",
]
_LAST_NAMES = [
    "Silva", "Santos", "Ferreira", "Costa", "Oliveira", "Pereira",
    "Rodrigues", "Almeida", "Martins", "Carvalho", "Lopes", "Gomes",
    "Sousa", "Pinto", "Neves",
]


def build_client():
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{config.MINIO_ENDPOINT}",
        aws_access_key_id=config.MINIO_ACCESS,
        aws_secret_access_key=config.MINIO_SECRET,
    )
    _ensure_bucket(client)
    print(f"[review_writer] Connected to MinIO at {config.MINIO_ENDPOINT}, bucket: {config.REVIEWS_BUCKET}")
    return client


def _ensure_bucket(client):
    try:
        client.head_bucket(Bucket=config.REVIEWS_BUCKET)
    except ClientError:
        client.create_bucket(Bucket=config.REVIEWS_BUCKET)


def _random_name() -> str:
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"


def _rating_expression(score: int) -> str:
    if random.random() < 0.5:
        return f"Dou {score} estrelas em 5."
    return f"Classifico este produto com {score}/5."


def _format_review(review: dict) -> str:
    name = _random_name()
    category = review.get("category", "geral")
    return (
        f"Boa tarde, o meu nome é {name} e venho partilhar a minha opinião "
        f"sobre um produto da categoria {category}.\n"
        f"{review['message']}\n"
        f"{_rating_expression(review['score'])}"
    )


def write_review(client, review: dict):
    date_str   = review["timestamp"][:10]
    object_key = f"{date_str}/{review['review_id']}_{review['order_id']}.txt"
    content    = _format_review(review)

    client.put_object(
        Bucket=config.REVIEWS_BUCKET,
        Key=object_key,
        Body=content.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )


def write_review_duplicate(client, review: dict):
    """Write the same review under a different file key.
    Simulates a user submitting the same review twice: both files reach Bronze
    with the same review_id, giving the Silver dedup logic something to do.
    """
    date_str   = review["timestamp"][:10]
    object_key = f"{date_str}/{review['review_id']}_{review['order_id']}_dup.txt"
    content    = _format_review(review)

    client.put_object(
        Bucket=config.REVIEWS_BUCKET,
        Key=object_key,
        Body=content.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )
