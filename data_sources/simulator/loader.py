"""
Loads Olist reference CSVs into memory.
The simulator draws from these pools to generate realistic events.
"""

import os
import csv
import random
from simulator import config


def _read_csv(filename: str) -> list[dict]:
    path = os.path.join(config.OLIST_DATA_PATH, filename)
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_reference_data() -> dict:
    print("[loader] Reading Olist reference CSVs...")

    products_raw  = _read_csv("olist_products_dataset.csv")
    customers_raw = _read_csv("olist_customers_dataset.csv")
    items_raw     = _read_csv("olist_order_items_dataset.csv")
    reviews_raw   = _read_csv("olist_order_reviews_dataset.csv")
    categories_raw = _read_csv("product_category_name_translation.csv")

    # category PT → EN map
    cat_translation = {
        row["product_category_name"]: row["product_category_name_english"]
        for row in categories_raw
    }

    # Products: keep only those with a known category and price reference
    price_by_product = {}
    seller_by_product = {}
    freight_by_product = {}
    for item in items_raw:
        pid = item["product_id"]
        if pid not in price_by_product:
            price_by_product[pid]   = float(item["price"])
            seller_by_product[pid]  = item["seller_id"]
            freight_by_product[pid] = float(item["freight_value"])

    products = []
    for row in products_raw:
        pid = row["product_id"]
        cat_pt = row.get("product_category_name", "") or ""
        if pid not in price_by_product or not cat_pt:
            continue
        products.append({
            "product_id":  pid,
            "category_pt": cat_pt,
            "category":    cat_translation.get(cat_pt, cat_pt),
            "price":       price_by_product[pid],
            "seller_id":   seller_by_product[pid],
            "freight":     freight_by_product[pid],
            "photos_qty":  int(row.get("product_photos_qty") or 1),
        })

    # Customers
    customers = [
        {
            "customer_id": row["customer_id"],
            "state":       row["customer_state"],
            "city":        row["customer_city"],
        }
        for row in customers_raw
    ]

    # Reviews with text only (unstructured source)
    reviews_with_text = [
        {
            "review_id":    row["review_id"],
            "review_score": int(row["review_score"]),
            "title":        row.get("review_comment_title", "").strip(),
            "message":      row.get("review_comment_message", "").strip(),
        }
        for row in reviews_raw
        if row.get("review_comment_message", "").strip()
    ]

    print(f"[loader] Products: {len(products):,} | Customers: {len(customers):,} | Reviews with text: {len(reviews_with_text):,}")

    return {
        "products":  products,
        "customers": customers,
        "reviews":   reviews_with_text,
    }


def pick_product(data: dict) -> dict:
    return random.choice(data["products"])


def pick_customer(data: dict) -> dict:
    return random.choice(data["customers"])


def pick_review_text(data: dict) -> dict:
    return random.choice(data["reviews"])
