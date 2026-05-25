"""
Controlled noise injection — simulates real-world data quality issues at the source.

Reviews (human text — highest noise rate):
  - invalid rating: out of range (0, 6, -1) or non-numeric ("N/A")
  - missing required fields: blank review_id, order_id, or customer_id
  - empty or whitespace-only message body
  - encoding corruption: UTF-8 chars misread as Latin-1 sequences
  - duplicate submission: same review written twice with different file key

Orders (structured DB via CDC — low noise rate):
  - inconsistent state format: wrong casing ("sp") or whitespace (" SP ")
  - invalid price: negative or zero
  - category with surrounding whitespace
  - CDC re-delivery: noop UPDATE triggers a second Debezium event for same order_id

Clickstream (browser-generated — minimal noise):
  - missing session_id (null)
  - unrecognised device value ("bot", "crawler", etc.)
  - missing event_type (null)
"""

import random
import copy

# ── Reviews ──────────────────────────────────────────────────────────────────
P_REVIEW_BAD_RATING    = 0.05
P_REVIEW_MISSING_FIELD = 0.04
P_REVIEW_EMPTY_MSG     = 0.03
P_REVIEW_ENCODING      = 0.07
P_REVIEW_DUPLICATE     = 0.05

# ── Orders ───────────────────────────────────────────────────────────────────
P_ORDER_BAD_PRICE      = 0.04
P_ORDER_BAD_STATE      = 0.06
P_ORDER_CATEGORY_SPACE = 0.05
P_ORDER_CDC_DUPLICATE  = 0.04

# ── Clickstream ──────────────────────────────────────────────────────────────
P_CLICK_NULL_SESSION   = 0.03
P_CLICK_INVALID_DEVICE = 0.04
P_CLICK_NULL_EVTYPE    = 0.02

_BAD_RATINGS     = [0, 6, -1, "N/A", 7]
_INVALID_DEVICES = ["bot", "crawler", "unknown_device", "tv", ""]

# UTF-8 sequences that get garbled when decoded as Latin-1
_ENCODING_CORRUPTIONS = [
    ("ã", "Ã£"),
    ("ç", "Ã§"),
    ("é", "Ã©"),
    ("ó", "Ã³"),
    ("ú", "Ãº"),
    ("â", "Ã¢"),
    ("ê", "Ãª"),
    ("à", "Ã "),
    ("í", "Ã­"),
    ("õ", "Ãµ"),
]


def _maybe(prob: float) -> bool:
    return random.random() < prob


def dirty_review(review: dict) -> tuple[dict, bool]:
    """
    Returns (dirty_review, should_duplicate).
    The duplicate flag tells the caller to write the same review a second time
    under a different file key — simulating a user submitting the review twice.
    """
    r = copy.deepcopy(review)
    duplicate = False

    if _maybe(P_REVIEW_BAD_RATING):
        r["score"] = random.choice(_BAD_RATINGS)

    if _maybe(P_REVIEW_MISSING_FIELD):
        field = random.choice(["review_id", "order_id", "customer_id"])
        r[field] = ""

    if _maybe(P_REVIEW_EMPTY_MSG):
        r["message"] = random.choice(["", "   ", ".", "-"])

    if _maybe(P_REVIEW_ENCODING) and r.get("message"):
        msg = r["message"]
        for clean, corrupted in _ENCODING_CORRUPTIONS:
            if clean in msg:
                msg = msg.replace(clean, corrupted, 1)
                break
        r["message"] = msg

    if _maybe(P_REVIEW_DUPLICATE):
        duplicate = True

    return r, duplicate


def dirty_purchase(purchase: dict) -> tuple[dict, bool]:
    """
    Returns (dirty_purchase, should_cdc_duplicate).
    The CDC duplicate flag tells the caller to issue a noop UPDATE on the row,
    which makes Debezium emit a second event for the same order_id.
    """
    p = copy.deepcopy(purchase)
    cdc_dup = False

    if _maybe(P_ORDER_BAD_PRICE):
        p["price"] = random.choice([-abs(p["price"]), 0.0, -1.0])

    if _maybe(P_ORDER_BAD_STATE):
        state = p.get("state", "")
        variant = random.randint(0, 2)
        if variant == 0:
            p["state"] = state.lower()
        elif variant == 1:
            p["state"] = f" {state} "
        else:
            p["state"] = state + "0"  # e.g. "SP0" — typo

    if _maybe(P_ORDER_CATEGORY_SPACE):
        p["category"] = f"  {p.get('category', '')}  "

    if _maybe(P_ORDER_CDC_DUPLICATE):
        cdc_dup = True

    return p, cdc_dup


def dirty_clickstream_events(events: list[dict]) -> list[dict]:
    """Applies noise in-place to a list of clickstream events."""
    for ev in events:
        if _maybe(P_CLICK_NULL_SESSION):
            ev["session_id"] = None
        if _maybe(P_CLICK_INVALID_DEVICE):
            ev["device"] = random.choice(_INVALID_DEVICES)
        if _maybe(P_CLICK_NULL_EVTYPE):
            ev["event_type"] = None
    return events
