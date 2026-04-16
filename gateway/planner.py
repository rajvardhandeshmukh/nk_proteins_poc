"""
Query Planner — Rule-Based with Confidence Scoring
====================================================
Converts natural language into structured intent + params + confidence.

Confidence Logic:
  0.95+ → Strong keyword match (exact domain term like "DSO", "dead stock")
  0.85  → Good keyword match (single clear signal like "revenue")
  0.70  → Weak/broad match (ambiguous terms like "performance", "stock")
  <0.70 → Falls to dynamic mode → LLM handles it in Phase 2B

The confidence score determines whether the system trusts itself
or defers to the LLM. This prevents confidently wrong answers.
"""

import re
import logging

from .sql_templates import VALID_INTENTS

logger = logging.getLogger(__name__)

# Confidence threshold: below this, fall back to dynamic/LLM
CONFIDENCE_THRESHOLD = 0.70

# =============================================================================
# SYNONYMS & KEYWORDS
# =============================================================================

REGION_KEYWORDS = {
    # Domestic (India)
    "gujarat": "Gujarat", "gujrat": "Gujarat", "gj": "Gujarat",
    "maharashtra": "Maharashtra", "mh": "Maharashtra", "mumbai": "Maharashtra",
    "rajasthan": "Rajasthan", "rj": "Rajasthan",
    "jammu and kashmir": "Jammu and Kashmir", "jammu kashmir": "Jammu and Kashmir", "jk": "Jammu and Kashmir",
    "jharkhand": "Jharkhand",
    "himachal pradesh": "Himachal Pradesh", "hp": "Himachal Pradesh",
    
    # International
    "milan": "Milan",
    "shanghai": "Shanghai", "shangai": "Shanghai",
    "export": "Export", "international": "Export",
    
    # Directions / Aliases
    "west": "Gujarat",
    "north": "Gujarat",
}

WAREHOUSE_KEYWORDS = {
    "mumbai": "Mumbai Depot", "bmbay": "Mumbai Depot",
    "ahmedabad": "Ahmedabad Depot", "amdabad": "Ahmedabad Depot",
    "rajkot": "Rajkot Warehouse",
    "vadodara": "Vadodara Depot", "baroda": "Vadodara Depot",
}

PRODUCT_KEYWORDS = {
    "cottonseed": "Cottonseed", "cotton": "Cottonseed",
    "sunflower": "Sunflower", "sun": "Sunflower",
    "groundnut": "Groundnut", "peanut": "Groundnut",
    "rice bran": "Rice Bran", "ricebran": "Rice Bran",
    "soyabean": "Soyabean", "soya": "Soyabean",
    "palmolein": "Palmolein", "palm": "Palmolein",
    "castor": "Castor",
}


# =============================================================================
# INTENT RULES — Each rule returns (intent, base_confidence, keyword_hits)
# =============================================================================

# Format: (keywords_list, intent, base_confidence)
# keywords_list: list of keyword groups. Each group is an OR-set.
# More keyword groups matched = higher final confidence.
INTENT_RULES = [
    # TIER 1 — Exact domain terms (0.95 base)
    {
        "intent": "dead_stock",
        "primary":   [["dead stock", "dead_stock", "non moving", "stagnant", "no movement"]],
        "boosters":  [["warehouse", "depot"], ["top", "worst"]],
        "base_conf": 0.95,
    },
    {
        "intent": "inventory_health",
        "primary":   [["inventory", "warehouse health", "reorder"]],
        "boosters":  [["warehouse", "depot"], ["health", "status", "report"]],
        "base_conf": 0.82,
    },
    {
        "intent": "worst_margins",
        "primary":   [["worst", "lowest", "negative"], ["margin", "profit"]],
        "boosters":  [["product"], ["region", "area"]],
        "base_conf": 0.93,
    },
    {
        "intent": "region_comparison",
        "primary":   [["region", "area", "zone", "state"]],
        "boosters":  [["revenue", "sales", "perform", "compar"]],
        "base_conf": 0.98,
    },
    {
        "intent": "top_products",
        "primary":   [["product", "item", "sku"], ["top", "best", "popular", "highest"]],
        "boosters":  [["region", "area"], ["revenue", "sales"]],
        "base_conf": 0.93,
    },
    # TIER 2 — Broad catch-all (lower base confidence)
    {
        "intent": "revenue_trend",
        "primary":   [["revenue", "sales", "trend", "growth"]],
        "boosters":  [["region", "area"], ["month", "quarter", "year"], ["product"]],
        "base_conf": 0.85,
    },
    {
        "intent": "cashflow_projection",
        "primary":   [["cashflow", "cash flow", "receivable", "outstanding", "aging", "payment"]],
        "boosters":  [["customer"], ["projection", "forecast", "status"]],
        "base_conf": 0.95,
    },
]


# =============================================================================
# MONTH LOOKUP (for date context extraction)
# =============================================================================

MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

# Words that signal a trend/rolling query (NOT a single point in time)
TREND_SIGNALS = {"trend", "growth", "last", "past", "recent", "over time", "compare"}


# =============================================================================
# EXTRACTION HELPERS
# =============================================================================

def _extract_date_context(text: str) -> dict:
    """
    Detect whether the user is asking about a specific point in time
    or a rolling trend.

    Returns:
        {
            "month": int or None,
            "year": int or None,
            "query_type": "point_in_time" | "trend"
        }
    """
    month = None
    year = None

    # Pattern 1: "February 2024", "feb 2024", "march 2025"
    match = re.search(
        r'\b(' + '|'.join(MONTH_MAP.keys()) + r')\s+(\d{4})\b', text
    )
    if match:
        month = MONTH_MAP[match.group(1)]
        year = int(match.group(2))

    # Pattern 2: "02/2024", "2/2024"
    if month is None:
        match = re.search(r'\b(\d{1,2})[/\-](\d{4})\b', text)
        if match:
            m = int(match.group(1))
            if 1 <= m <= 12:
                month = m
                year = int(match.group(2))

    # Pattern 3: standalone year "in 2024" (no month)
    if year is None:
        match = re.search(r'\b(20\d{2})\b', text)
        if match:
            year = int(match.group(1))

    # Determine query type
    has_trend_signal = any(s in text for s in TREND_SIGNALS)

    if month is not None and year is not None and not has_trend_signal:
        query_type = "point_in_time"
    else:
        query_type = "trend"

    return {"month": month, "year": year, "query_type": query_type}


def _extract_limit(text: str) -> int:
    match = re.search(r'(?:top|bottom|worst|best|first|last|only)\s+(\d+)', text)
    return int(match.group(1)) if match else 10


def _extract_months(text: str) -> int:
    match = re.search(r'(?:last|past)\s+(\d+)\s+months?', text)
    if match:
        return int(match.group(1))
    if "quarter" in text:
        return 3
    if "year" in text:
        return 12
    if "half year" in text or "6 month" in text:
        return 6
    return 12


def _extract_region(text: str):
    for keyword, value in REGION_KEYWORDS.items():
        if keyword in text:
            return value
    return None


def _extract_warehouse(text: str):
    for keyword, value in WAREHOUSE_KEYWORDS.items():
        if keyword in text:
            return value
    return None


def _extract_product(text: str):
    # Priority 1: Check for full known product prefixes (like SUNPRIDE)
    if "sunprime" in text or "sunpride" in text:
        # Try to find the whole segment
        match = re.search(r'(sunpride\s+[a-z0-9\s]+(?:nt|ltr|kg))', text)
        if match:
            return match.group(1).upper()
            
    # Priority 2: Keyword fallback
    for keyword, value in PRODUCT_KEYWORDS.items():
        if keyword in text:
            return value
    return None


# =============================================================================
# CONFIDENCE SCORER
# =============================================================================

def _score_rule(text: str, rule: dict) -> float:
    """
    Score how well a user's text matches a rule.

    Logic:
    1. ALL primary keyword groups must have at least one hit.
       If any primary group has zero hits → score = 0 (no match).
    2. Each booster group that hits adds +0.02 to confidence.
    3. Final confidence is clamped to [0, 1.0].
    """
    # Check primary groups — ALL must match
    for group in rule["primary"]:
        if not any(kw in text for kw in group):
            return 0.0  # Failed a required keyword group

    # Base confidence (all primaries matched)
    conf = rule["base_conf"]

    # Booster keywords add incremental confidence
    for group in rule.get("boosters", []):
        if any(kw in text for kw in group):
            conf += 0.02

    return min(conf, 1.0)


# =============================================================================
# MAIN PLANNER
# =============================================================================

def plan_query(user_input: str) -> dict:
    """
    Convert natural language → structured plan with confidence score.

    Returns:
        {
            "intent": "revenue_trend",
            "params": {"months_back": 6, "region": "Gujarat"},
            "mode": "template",     # or "dynamic" if confidence < threshold
            "confidence": 0.87
        }
    """
    text = user_input.lower().strip()

    # --- Score all rules ---
    scored = []
    for rule in INTENT_RULES:
        score = _score_rule(text, rule)
        if score > 0:
            scored.append((rule["intent"], score))

    # Sort by confidence descending
    scored.sort(key=lambda x: x[1], reverse=True)

    # --- Pick the best match ---
    if scored:
        best_intent, best_conf = scored[0]

        # Ambiguity penalty: if top 2 are close in score, reduce confidence
        if len(scored) >= 2:
            gap = scored[0][1] - scored[1][1]
            if gap < 0.05:
                best_conf -= 0.10  # Significant penalty for ambiguity
                logger.info(
                    "Ambiguity detected: '%s' (%.2f) vs '%s' (%.2f) — penalizing to %.2f",
                    scored[0][0], scored[0][1], scored[1][0], scored[1][1], best_conf
                )

        best_conf = round(best_conf, 2)
    else:
        best_intent = "unknown"
        best_conf = 0.0

    # --- Extract date context ---
    date_context = _extract_date_context(text)

    # --- Extract params ---
    region = _extract_region(text)
    warehouse = _extract_warehouse(text)
    product = _extract_product(text)
    months = _extract_months(text)
    limit = _extract_limit(text)

    params = {}

    # Intent-specific param assembly
    if best_intent in ("revenue_trend",):
        params["months_back"] = months
        if region:
            params["region"] = region
        if product:
            params["product"] = product

    elif best_intent in ("top_products", "top_customers", "worst_margins"):
        params["limit"] = limit
        params["months_back"] = months
        if region:
            params["region"] = region

    elif best_intent in ("slow_payers", "overdue_invoices"):
        params["limit"] = limit
        if region:
            params["region"] = region

    elif best_intent in ("dead_stock",):
        params["limit"] = limit
        if warehouse:
            params["warehouse"] = warehouse

    elif best_intent in ("inventory_health",):
        if warehouse:
            params["warehouse"] = warehouse

    elif best_intent in ("receivables_aging", "dso"):
        if region:
            params["region"] = region

    elif best_intent in ("region_comparison",):
        params["months_back"] = months
        if date_context["month"] and date_context["year"]:
            params["month"] = f"{date_context['year']}-{date_context['month']:02d}"

    elif best_intent in ("gst_mismatch",):
        params["limit"] = limit

    # gst_summary has no params

    # --- Determine mode ---
    if best_conf < CONFIDENCE_THRESHOLD:
        mode = "dynamic"
        logger.info(
            "Low confidence (%.2f) for '%s' on query: '%s' → falling back to dynamic",
            best_conf, best_intent, user_input
        )
    else:
        mode = "template"

    # --- CRITICAL RULE: Point-in-time override ---
    # If user asked for a specific month+year (e.g., "February 2024"),
    # the rolling-window templates CANNOT serve this. Force dynamic mode
    # so the LLM SQL generator writes an exact WHERE clause.
    if date_context["query_type"] == "point_in_time" and mode == "template":
        logger.info(
            "Point-in-time query detected (month=%s, year=%s). "
            "Staying in template mode with hardened v3 filtering.",
            date_context["month"], date_context["year"]
        )
        if date_context["month"] and date_context["year"]:
            params["month"] = f"{date_context['year']}-{date_context['month']:02d}"

    return {
        "intent": best_intent,
        "params": params,
        "mode": mode,
        "confidence": best_conf,
        "original_query": user_input,
        "date_context": date_context,
    }
