"""
Governance Module — Guardrails & Reliability (Floor 3)
======================================================
This module ensures we don't just "show data" but "show audited data".
It adds context, reliability scores, and safety flags to every response.
"""

from enum import Enum
from typing import List, Optional

class ReliabilityLevel(str, Enum):
    HIGH = "HIGH"      # Official P&L, Audit-grade (fact_profitability, fact_cashflow)
    MEDIUM = "MEDIUM"  # Operational proxy (fact_sales + inventory cost fallback)
    LOW = "LOW"       # Estimated/Dynamic SQL without pre-audited templates

def get_reliability(intent: str) -> ReliabilityLevel:
    """Assign a reliability tier based on the intent's data source."""
    high_tier = {
        "product_profitability", 
        "top_profitable_products", 
        "loss_making_products", 
        "financial_margin_trend",
        "cashflow_projection",
        "outstanding_receivables",
        "aging_distribution",
        "collection_efficiency",
        "bom_lookup"
    }
    
    medium_tier = {
        "revenue_trend",
        "top_products",
        "region_comparison",
        "inventory_health",
        "reorder_alerts",
        "dead_stock",
        "worst_margins",
        "top_margins"
    }

    if intent in high_tier:
        return ReliabilityLevel.HIGH
    if intent in medium_tier:
        return ReliabilityLevel.MEDIUM
    return ReliabilityLevel.LOW

def get_governance_notes(intent: str, data: dict) -> List[str]:
    """Provide business context and caveats for specific intents."""
    notes = []
    
    if intent in ("revenue_trend", "top_products"):
        notes.append("Revenue is Net Sales (post-discount, but PRE-TAX).")
        notes.append("Returns are tracked but NOT subtracted from revenue in this view.")
        
    if intent in ("worst_margins", "top_margins"):
        notes.append("Cost is an estimate using average unit cost from Inventory.")
        notes.append("Not for official financial reporting.")

    if intent == "inventory_health":
        notes.append("Inventory is at Plant + Storage Location grain.")
        
    return notes

def detect_conflicts(sample_row: dict) -> List[str]:
    """Detect data integrity issues in a single row of data."""
    conflicts = []
    
    # Check for negative stock
    if "current_stock" in sample_row and sample_row["current_stock"] < 0:
        conflicts.append(f"NEGATIVE STOCK DETECTED: {sample_row.get('product_name')} ({sample_row.get('current_stock')})")
        
    # Check for zero cost in profitability
    if "total_cogs" in sample_row and sample_row["total_cogs"] == 0:
        if sample_row.get("total_net_revenue", 0) > 0:
            conflicts.append("COST MISSING: Revenue exists but COGS is 0.")

    return conflicts
