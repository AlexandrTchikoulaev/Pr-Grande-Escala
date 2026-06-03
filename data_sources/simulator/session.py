"""
Simulates a single user session following the e-commerce event funnel.

Returns:
  clickstream_events  — list of dicts  → Kafka
  purchase            — dict or None   → PostgreSQL
  review              — dict or None   → MinIO (.txt)
"""

import uuid
import random
from datetime import datetime, timedelta
from simulator import config, loader


def _now_str(dt: datetime) -> str:
    return dt.isoformat()


def _next_ts(current: datetime) -> datetime:
    delta = random.randint(config.EVENT_DELAY_MIN, config.EVENT_DELAY_MAX)
    return current + timedelta(seconds=delta)


def _event(session_id: str, user_id: str, device: str,
           event_type: str, ts: datetime, props: dict) -> dict:
    return {
        "event_id":   str(uuid.uuid4()),
        "session_id": session_id,
        "user_id":    user_id,
        "event_type": event_type,
        "timestamp":  _now_str(ts),
        "device":     device,
        "properties": props,
    }


def simulate_session(data: dict) -> tuple[list[dict], dict | None, dict | None]:
    session_id = str(uuid.uuid4())
    customer   = loader.pick_customer(data)
    user_id    = customer["customer_id"]
    device     = random.choices(config.DEVICES, weights=config.DEVICE_WEIGHTS)[0]

    ts = datetime.utcnow()
    events: list[dict] = []
    purchase = None
    review   = None

    # --- session_start ---
    events.append(_event(session_id, user_id, device, "session_start", ts, {
        "state": customer["state"],
        "city":  customer["city"],
    }))

    # --- navigation: search or category_browse ---
    ts = _next_ts(ts)
    product = loader.pick_product(data)
    if random.random() < config.P_SEARCH:
        events.append(_event(session_id, user_id, device, "search", ts, {
            "query":    product["category"],
            "category": product["category"],
        }))
    else:
        events.append(_event(session_id, user_id, device, "category_browse", ts, {
            "category": product["category"],
        }))

    # --- bounce check ---
    if random.random() > config.P_PRODUCT_VIEW:
        ts = _next_ts(ts)
        events.append(_event(session_id, user_id, device, "session_end", ts, {"reason": "bounce"}))
        return events, None, None

    # --- product_view ---
    ts = _next_ts(ts)
    events.append(_event(session_id, user_id, device, "product_view", ts, {
        "product_id": product["product_id"],
        "category":   product["category"],
        "price":      product["price"],
        "photos_qty": product["photos_qty"],
    }))

    # --- product_review_read (optional) ---
    if random.random() < config.P_REVIEW_READ:
        ts = _next_ts(ts)
        events.append(_event(session_id, user_id, device, "product_review_read", ts, {
            "product_id": product["product_id"],
            "category":   product["category"],
        }))

    # --- add_to_cart? ---
    if random.random() > config.P_ADD_TO_CART:
        ts = _next_ts(ts)
        events.append(_event(session_id, user_id, device, "session_end", ts, {"reason": "no_cart"}))
        return events, None, None

    # --- remove_from_cart (optional, then re-add) ---
    if random.random() < config.P_REMOVE_FROM_CART:
        ts = _next_ts(ts)
        events.append(_event(session_id, user_id, device, "remove_from_cart", ts, {
            "product_id": product["product_id"],
        }))
        ts = _next_ts(ts)
        events.append(_event(session_id, user_id, device, "add_to_cart", ts, {
            "product_id": product["product_id"],
            "category":   product["category"],
            "price":      product["price"],
        }))
    else:
        ts = _next_ts(ts)
        events.append(_event(session_id, user_id, device, "add_to_cart", ts, {
            "product_id": product["product_id"],
            "category":   product["category"],
            "price":      product["price"],
        }))

    # --- cart_view ---
    ts = _next_ts(ts)
    events.append(_event(session_id, user_id, device, "cart_view", ts, {
        "product_id": product["product_id"],
        "total":      round(product["price"] + product["freight"], 2),
    }))

    # --- cart_abandon? ---
    if random.random() < config.P_CART_ABANDON:
        ts = _next_ts(ts)
        events.append(_event(session_id, user_id, device, "cart_abandon", ts, {
            "product_id": product["product_id"],
        }))
        ts = _next_ts(ts)
        events.append(_event(session_id, user_id, device, "session_end", ts, {"reason": "cart_abandon"}))
        return events, None, None

    # --- checkout_start ---
    ts = _next_ts(ts)
    events.append(_event(session_id, user_id, device, "checkout_start", ts, {
        "product_id": product["product_id"],
        "total":      round(product["price"] + product["freight"], 2),
    }))

    # --- order placed? ---
    if random.random() > config.P_ORDER_PLACED:
        ts = _next_ts(ts)
        events.append(_event(session_id, user_id, device, "session_end", ts, {"reason": "checkout_abandon"}))
        return events, None, None

    # --- order_placed (goes to Kafka + PostgreSQL relational DB) ---
    order_id      = str(uuid.uuid4())
    order_item_id = str(uuid.uuid4())
    ts = _next_ts(ts)
    events.append(_event(session_id, user_id, device, "order_placed", ts, {
        "order_id":   order_id,
        "product_id": product["product_id"],
        "total":      round(product["price"] + product["freight"], 2),
    }))

    purchase = {
        "order_id":           order_id,
        "order_item_id":      order_item_id,
        "session_id":         session_id,
        "customer_id":        user_id,
        "city":               customer["city"],
        "product_id":         product["product_id"],
        "seller_id":          product["seller_id"],
        "category":           product["category"],
        "photos_qty":         product["photos_qty"],
        "price":              product["price"],
        "freight_value":      product["freight"],
        "purchase_timestamp": _now_str(ts),
        "state":              customer["state"],
    }

    ts = _next_ts(ts)
    events.append(_event(session_id, user_id, device, "session_end", ts, {"reason": "completed"}))

    # --- review_submitted (optional, delayed) ---
    if random.random() < config.P_REVIEW_SUBMIT:
        review_ref = loader.pick_review_text(data)
        delay = random.randint(config.REVIEW_DELAY_MIN, config.REVIEW_DELAY_MAX)
        review_ts = ts + timedelta(seconds=delay)
        review = {
            "review_id":  str(uuid.uuid4()),
            "order_id":   order_id,
            "customer_id": user_id,
            "score":      review_ref["review_score"],
            "title":      review_ref["title"],
            "message":    review_ref["message"],
            "category":   product["category"],
            "timestamp":  _now_str(review_ts),
            "delay_seconds": delay,
        }

    return events, purchase, review
