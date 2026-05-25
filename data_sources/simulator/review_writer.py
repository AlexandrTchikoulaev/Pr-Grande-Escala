"""
MinIO writer — stores reviews as raw .txt files.
This represents the unstructured data source: free-text reviews that
require NLP to extract meaning, in contrast to the structured and
semi-structured sources.

File path convention:
  raw-reviews/{YYYY-MM-DD}/{order_id}.txt
"""

import io
import boto3
from botocore.exceptions import ClientError
from simulator import config


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


def _format_review(review: dict) -> str:
    lines = [
        f"REVIEW_ID: {review['review_id']}",
        f"ORDER_ID: {review['order_id']}",
        f"CUSTOMER_ID: {review['customer_id']}",
        f"RATING: {review['score']}/5",
        f"TIMESTAMP: {review['timestamp']}",
        "---",
    ]
    if review["title"]:
        lines.append(f"TITLE: {review['title']}")
    lines.append("")
    lines.append(review["message"])
    return "\n".join(lines)


def write_review(client, review: dict):
    date_str   = review["timestamp"][:10]           # YYYY-MM-DD
    object_key = f"{date_str}/{review['order_id']}.txt"
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
    with the same review_id inside, giving the Silver dedup logic something to do.
    """
    date_str   = review["timestamp"][:10]
    object_key = f"{date_str}/{review['review_id']}-dup.txt"
    content    = _format_review(review)

    client.put_object(
        Bucket=config.REVIEWS_BUCKET,
        Key=object_key,
        Body=content.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )
