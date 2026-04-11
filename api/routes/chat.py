"""Chat endpoint — pipeline assistant with Ollama LLM + rule-based fallback."""
from __future__ import annotations

import json
import os
import re
from typing import Any

import ollama
from dotenv import load_dotenv
from fastapi import APIRouter
from pydantic import BaseModel

from utils.logger import get_logger

load_dotenv()

logger = get_logger("amdais.chat")
router = APIRouter(tags=["chat"])

OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral")
OLLAMA_HOST = os.getenv("OLLAMA_HOST")
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY")

_client_kwargs: dict = {"host": OLLAMA_HOST} if OLLAMA_HOST else {}
if OLLAMA_API_KEY:
    _client_kwargs["headers"] = {"Authorization": f"Bearer {OLLAMA_API_KEY}"}

try:
    OLLAMA_CLIENT: ollama.Client | None = ollama.Client(**_client_kwargs)
except Exception:
    OLLAMA_CLIENT = None

DOMAINS = [
    "mining", "healthcare", "ecommerce", "finance",
    "manufacturing", "logistics", "agriculture", "energy",
]

STRATEGIES = ["none", "mean", "median", "zero", "drop"]


class ChatRequest(BaseModel):
    message: str
    pipeline_state: dict = {}
    context: dict = {}


# ── Rule-based fallback ────────────────────────────────────────────────────────

def _rule_based_parse(message: str, pipeline_state: dict, context: dict) -> dict:
    """Deterministic intent parser — runs when Ollama is unavailable."""
    msg = message.lower().strip()
    updates: dict[str, Any] = {}
    stage = "structure"
    reply_parts: list[str] = []

    # Missing-value strategy
    for strategy in STRATEGIES:
        if strategy in msg and "strategy" not in msg.split(strategy)[0][-10:]:
            updates["missing_strategy"] = strategy
            reply_parts.append(f"Set missing-value strategy to **{strategy}**.")
            stage = "structure"
            break

    # Domain override
    for domain in DOMAINS:
        if domain in msg:
            updates["domain_override"] = domain
            reply_parts.append(f"Domain locked to **{domain}**.")
            stage = "detect"
            break

    # Focus columns  — "focus on X, Y"
    focus_m = re.search(r"focus\s+(?:on\s+)?([\w\s,]+?)(?:\s+column|\s+field|$)", msg)
    if focus_m:
        raw = focus_m.group(1)
        cols = [c.strip() for c in re.split(r"[,&]+", raw) if c.strip()]
        if cols:
            updates["focus_columns"] = ", ".join(cols)
            reply_parts.append(f"Analysis focused on: {', '.join(cols)}.")
            stage = "analyze"

    # Exclude / ignore columns
    excl_m = re.search(r"(?:ignore|exclude|remove|drop)\s+(?:column\s+)?([\w\s,]+?)(?:\s+column|$)", msg)
    if excl_m:
        raw = excl_m.group(1)
        cols = [c.strip() for c in re.split(r"[,&]+", raw) if c.strip()]
        if cols:
            updates["exclude_columns"] = ", ".join(cols)
            reply_parts.append(f"Excluded columns: {', '.join(cols)}.")
            stage = "detect" if not focus_m else stage

    # Severity filter
    for sev in ["critical", "warning", "info"]:
        if f"only {sev}" in msg or f"minimum {sev}" in msg or f"filter to {sev}" in msg:
            updates["min_severity"] = sev.upper()
            reply_parts.append(f"Insights filtered to **{sev.upper()}** and above.")
            stage = "insight"
            break

    # Insight count — numeric or relative
    num_m = re.search(r"(\d+)\s+insights?", msg)
    if re.search(r"(?:more\s+insights?|increase\s+insights?)", msg):
        cur = int(pipeline_state.get("insight", {}).get("insight_count", 6))
        updates["insight_count"] = min(cur + 2, 12)
        reply_parts.append(f"Increased insight count to {updates['insight_count']}.")
        stage = "insight"
    elif re.search(r"(?:fewer\s+insights?|less\s+insights?|reduce\s+insights?)", msg):
        cur = int(pipeline_state.get("insight", {}).get("insight_count", 6))
        updates["insight_count"] = max(cur - 2, 2)
        reply_parts.append(f"Reduced insight count to {updates['insight_count']}.")
        stage = "insight"
    elif num_m and "insight" in msg:
        updates["insight_count"] = max(2, min(12, int(num_m.group(1))))
        reply_parts.append(f"Set insight count to {updates['insight_count']}.")
        stage = "insight"

    # Outlier threshold
    if "sensitive" in msg and "outlier" in msg:
        updates["outlier_threshold"] = 1.5
        reply_parts.append("Outlier threshold set to 1.5× IQR (sensitive).")
        stage = "analyze"
    elif "lenient" in msg and "outlier" in msg:
        updates["outlier_threshold"] = 3.0
        reply_parts.append("Outlier threshold set to 3.0× IQR (lenient).")
        stage = "analyze"

    # Insight style
    for style in ["concise", "detailed", "executive"]:
        if style in msg and "insight" in msg:
            updates["insight_style"] = style
            reply_parts.append(f"Insight style set to **{style}**.")
            stage = "insight"
            break

    # Fallback to simple context Q&A or unparsed
    if not updates:
        # Check if it's a data question
        if "correlation" in msg and context:
            corrs = context.get("correlation_top", [])
            if corrs:
                c = corrs[0]
                return {"reply": f"The strongest correlation is between {c['left']} and {c['right']} (r={c['corr']:.2f}).", "stage": None, "updates": {}}
        if "outlier" in msg and context:
            outs = context.get("outlier_scan", [])
            if outs:
                o = outs[0]
                return {"reply": f"The column with most outliers is {o['column']} ({o['outlier_pct']:.1%} flagged).", "stage": None, "updates": {}}
        if ("rows" in msg or "columns" in msg) and context:
            ov = context.get("overview", {})
            return {"reply": f"The dataset has {context.get('rows', 0):,} rows and {ov.get('columns', 0)} columns.", "stage": None, "updates": {}}
        
        return {
            "reply": (
                "I couldn't parse a specific control from that. "
                "Try things like: 'use median imputation', 'focus on revenue', "
                "or ask questions like 'what is the strongest correlation?'"
            ),
            "stage": None,
            "updates": {},
        }

    reply = " ".join(reply_parts) + " Click **Re-run** to apply these changes."
    return {"reply": reply, "stage": stage, "updates": updates}


# ── Ollama with retry ──────────────────────────────────────────────────────────

_SYSTEM_PROMPT = (
    "You are an expert AI data analyst assistant. You have two strict modes of operation. "
    "You MUST respond ONLY with a valid single JSON object. Do not add markdown formatting or explanations outside the JSON.\n\n"
    "--- MODE 1: PIPELINE CONTROL (User wants to change settings) ---\n"
    "If the user says e.g. 'use median imputation', 'focus on revenue', 'domain is finance'.\n"
    "Settings you can change:\n"
    "  detect:    domain_override, exclude_columns\n"
    "  structure: missing_strategy (none|mean|median|zero|drop), outlier_handling\n"
    "  analyze:   focus_columns, correlation_depth, outlier_threshold (1.5|2.0|3.0)\n"
    "  insight:   min_severity (INFO|WARNING|CRITICAL), insight_count, insight_style\n"
    "Return JSON format: { \"reply\": \"Confirmation message\", \"stage\": \"stage_name\", \"updates\": { \"control\": \"value\" } }\n\n"
    "--- MODE 2: DATASET Q&A (User asks a question about the data) ---\n"
    "If the user asks e.g. 'what is the strongest correlation?', 'how many rows?', 'are there outliers?'\n"
    "Read the provided 'Analysis context' JSON carefully. It contains data profiles, correlations, outliers, missingness, and row counts.\n"
    "Answer the user's question directly using ONLY the numbers and facts found in the 'Analysis context'. "
    "Return JSON format: { \"reply\": \"Your specific data-backed answer\", \"stage\": null, \"updates\": {} }"
)


def _try_ollama(message: str, pipeline_state: dict, context: dict) -> dict | None:
    """Attempt Ollama with up to 2 retries. Returns None when unavailable."""
    if OLLAMA_CLIENT is None:
        return None

    user_content = (
        f"Current pipeline state: {json.dumps(pipeline_state)}\n"
        f"Analysis context: {json.dumps(context)}\n"
        f"User instruction: \"{message}\""
    )

    for attempt in range(2):
        try:
            response = OLLAMA_CLIENT.chat(
                model=OLLAMA_MODEL,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                format="json",
                options={"num_ctx": 2048, "num_predict": 1024, "num_thread": 8},
                keep_alive="1h",
            )
            raw = response["message"]["content"]
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and "reply" in parsed:
                return parsed
        except Exception as exc:
            logger.warning("Ollama chat attempt %d failed: %s", attempt + 1, exc)
            if attempt == 1:
                return None
    return None


# ── Route ─────────────────────────────────────────────────────────────────────

@router.post("/chat")
def chat_endpoint(body: ChatRequest) -> dict:
    result = _try_ollama(body.message, body.pipeline_state, body.context)
    if result is None:
        logger.info("Ollama unavailable — using rule-based fallback for chat")
        result = _rule_based_parse(body.message, body.pipeline_state, body.context)
        result["fallback"] = True
    else:
        result["fallback"] = False
        # Normalize: ensure updates is always a dict
        if not isinstance(result.get("updates"), dict):
            result["updates"] = {}
    return result
