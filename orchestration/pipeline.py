
# orchestration/pipeline.py
# Unified deterministic orchestrator for the document intelligence system

from typing import List, Dict, Any
from collections import defaultdict, Counter
from dataclasses import dataclass

import sqlite3
import pickle
from scipy.sparse import vstack

from analytics.loader import load_sentences_with_context
from decision.role_classifier import classify_sentence
from decision.chain_builder import build_chains
from decision.metrics import decision_coverage, chain_completeness
from decision.debt import analyze_chains
from engine import CaseIndex

DB_PATH = "storage/data_lake.db"


# ============================================================
# Errors
# ============================================================

class PipelineError(Exception):
    """Controlled pipeline failure"""
    pass


# ============================================================
# Ingestion Pipeline (wrapper)
# ============================================================

def run_ingestion_pipeline(ingest_callable):
    """
    Wrapper for ingestion.
    Keeps orchestration uniform even though ingestion
    is mostly UI-driven.
    """
    try:
        ingest_callable()
    except Exception as e:
        raise PipelineError(f"Ingestion failed: {e}")


# ============================================================
# Query Pipeline
# ============================================================

def run_query_pipeline(context: str, query: str) -> Dict[str, Any]:
    """
    Deterministic query pipeline.
    """
    if not context or not query:
        raise PipelineError("Context and query are required.")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # --------------------------------------------------
    # Load vectorizer
    # --------------------------------------------------
    row = cur.execute(
        "SELECT vectorizer FROM vectorizers WHERE context = ?",
        (context,),
    ).fetchone()

    if not row:
        conn.close()
        raise PipelineError("Vectorizer not found for context.")

    vectorizer = pickle.loads(row[0])

    # --------------------------------------------------
    # Load sentences & vectors
    # --------------------------------------------------
    rows = cur.execute(
        """
        SELECT s.sentence_text, s.vector
        FROM sentences s
        JOIN documents d ON s.document_id = d.document_id
        WHERE d.context = ?
        ORDER BY d.document_id, s.sentence_index
        """,
        (context,),
    ).fetchall()

    conn.close()

    if not rows:
        raise PipelineError("No sentences found for context.")

    sentences: List[str] = []
    vectors = []

    for text, vec in rows:
        sentences.append(text)
        vectors.append(pickle.loads(vec))

    # --------------------------------------------------
    # Build index
    # --------------------------------------------------
    index = CaseIndex()
    index.sentences = sentences
    index.matrix = vstack(vectors)
    index.vectorizer = vectorizer

    # --------------------------------------------------
    # Query
    # --------------------------------------------------
    results = index.query(query)

    return {
        "results": results,
        "total_sentences": len(sentences),
    }


# ============================================================
# Deterministic unified decision helpers (LLM-free)
# ============================================================

@dataclass(frozen=True)
class Chain:
    context: str
    cause: str | None
    action: str | None
    outcome: str | None
    timestamp: str | None  # ISO or None
    constraints: list[str] | None  # parsed structural constraints if available


def _normalize(s: str | None) -> str | None:
    return s.strip().lower() if isinstance(s, str) else None


# --- Role aliases (lowercase) ---
CAUSE_KEYS = {
    "cause", "causes", "root cause", "root causes", "problem", "issue", "why", "reason"
}
ACTION_KEYS = {
    "action", "actions", "mitigation", "mitigations", "fix", "fixes",
    "remediation", "remediations", "change", "changes", "countermeasure",
    "countermeasures", "solution", "solutions", "step", "steps", "task", "tasks"
}
OUTCOME_KEYS = {
    "outcome", "outcomes", "result", "results", "impact", "impacts",
    "effect", "effects", "evidence", "verification", "validation",
    "metric", "metrics", "kpi", "kpis", "learning", "lessons"
}
TS_KEYS = {"timestamp", "ts", "time", "datetime", "date"}
CONSTRAINT_KEYS = {
    "constraint", "constraints", "assumption", "assumptions",
    "limitation", "limitations", "guardrail", "guardrails",
    "precondition", "preconditions", "policy", "policies", "sla", "slo"
}

def _first_text(val: Any) -> str | None:
    """Return a single string from str/list/tuple/dict/scalar, else None (deterministic, no inference)."""
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return s if s else None
    if isinstance(val, (list, tuple)):
        parts = [str(x).strip() for x in val if isinstance(x, (str, int, float)) and str(x).strip()]
        return " | ".join(parts) if parts else None
    if isinstance(val, dict):
        parts = [str(v).strip() for v in val.values() if isinstance(v, (str, int, float)) and str(v).strip()]
        return " | ".join(parts) if parts else None
    if isinstance(val, (int, float)):
        return str(val)
    return None

def _find_key(original: dict, aliases: set[str]) -> str | None:
    """Find a key in original dict whose lowercase matches any alias (plural tolerant)."""
    if not original:
        return None
    lower_map = {k.lower(): k for k in original.keys()}
    for alias in aliases:
        if alias in lower_map:
            return lower_map[alias]
    # also try stripping trailing 's'
    for lk, ok in lower_map.items():
        if lk.endswith('s') and lk[:-1] in aliases:
            return ok
    return None

def _extract_roles(c: Dict[str, Any]) -> tuple[str | None, str | None, str | None, str | None, list[str] | None]:
    """
    Best-effort role extraction tolerant to casing, plurals, and synonyms.
    Returns (cause, action, outcome, timestamp, constraints)
    """
    if not isinstance(c, dict):
        return None, None, None, None, None

    cause_key = _find_key(c, CAUSE_KEYS)
    action_key = _find_key(c, ACTION_KEYS)
    outcome_key = _find_key(c, OUTCOME_KEYS)
    ts_key = _find_key(c, TS_KEYS)
    constraints_key = _find_key(c, CONSTRAINT_KEYS)

    cause = _first_text(c.get(cause_key)) if cause_key else None
    action = _first_text(c.get(action_key)) if action_key else None
    outcome = _first_text(c.get(outcome_key)) if outcome_key else None
    timestamp = _first_text(c.get(ts_key)) if ts_key else None

    constraints_val = c.get(constraints_key) if constraints_key else None
    constraints: list[str] | None = None
    if isinstance(constraints_val, list):
        parts = [_first_text(x) for x in constraints_val]
        constraints = [p for p in parts if p] or None
    else:
        one = _first_text(constraints_val)
        constraints = [one] if one else None

    return cause, action, outcome, timestamp, constraints


def _score_actions(chains: List[Chain], weights: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    """
    Aggregate actions across contexts and compute deterministic scorecards.
    """
    by_action = defaultdict(lambda: {
        "chains": [],
        "contexts": set(),
        "causes": 0,
        "causes_with_action": 0,
        "actions_with_outcome": 0,
        "total_actions": 0,
        "latest_ts": None,
        "constraint_hits": 0,
        "constraint_total": 0,
        "debt_broken": 0,
        "debt_action_only": 0,
    })

    def newer(a, b):
        if not a:
            return b
        if not b:
            return a
        return max(a, b)

    for ch in chains:
        a = _normalize(ch.action)
        if not a:
            continue
        rec = by_action[a]
        rec["chains"].append(ch)
        rec["contexts"].add(ch.context)
        rec["total_actions"] += 1
        rec["latest_ts"] = newer(rec["latest_ts"], ch.timestamp)

        if ch.cause:
            rec["causes"] += 1
            rec["causes_with_action"] += 1
        else:
            rec["debt_action_only"] += 1

        if ch.outcome:
            rec["actions_with_outcome"] += 1
        else:
            rec["debt_broken"] += 1

        # constraints scoring: treat presence as requirements; action must not violate
        if ch.constraints is not None:
            rec["constraint_total"] += 1
            fits = True  # plug in evaluator if available
            if fits:
                rec["constraint_hits"] += 1

    scored: Dict[str, Dict[str, Any]] = {}
    for a, r in by_action.items():
        support = len(r["contexts"])
        cause_cov = (r["causes_with_action"] / max(1, r["causes"])) if r["causes"] else 0.0
        outcome_strength = r["actions_with_outcome"] / max(1, r["total_actions"])
        constraint_fit = (r["constraint_hits"] / max(1, r["constraint_total"])) if r["constraint_total"] else 1.0
        recency = 1.0 if r["latest_ts"] else 0.0
        debt = (r["debt_broken"] + r["debt_action_only"]) / max(1, r["total_actions"])

        score = (
            weights["w1"] * support +
            weights["w2"] * cause_cov +
            weights["w3"] * outcome_strength +
            weights["w4"] * recency +
            weights["w5"] * constraint_fit -
            weights["w6"] * debt
        )

        scored[a] = {
            "score": round(float(score), 4),
            "support": support,
            "cause_coverage": round(cause_cov, 3),
            "outcome_strength": round(outcome_strength, 3),
            "recency": recency,
            "constraint_fit": round(constraint_fit, 3),
            "debt_penalty": round(debt, 3),
            "contexts": sorted(r["contexts"]),
            "sample_chains": [
                {
                    "context": ch.context,
                    "cause": ch.cause,
                    "action": ch.action,
                    "outcome": ch.outcome,
                    "timestamp": ch.timestamp,
                } for ch in r["chains"][:5]
            ],
        }

    return scored


def _pick_unified(scored: Dict[str, Dict[str, Any]]) -> Dict[str, Any] | None:
    if not scored:
        return None
    ranked = sorted(
        scored.items(),
        key=lambda kv: (
            -kv[1]["score"],
            -kv[1]["outcome_strength"],
            -kv[1]["constraint_fit"],
            -kv[1]["cause_coverage"],
            -len(kv[1]["contexts"]),
            kv[1]["debt_penalty"],
        ),
    )
    top_action, details = ranked[0]
    return {
        "action": top_action,
        "scorecard": details,
        "alternatives": [
            {"action": a, "scorecard": d} for a, d in ranked[1:4]
        ],
    }


# ============================================================
# Decision Intelligence Pipeline
# ============================================================

def run_decision_pipeline(selected_contexts: List[str]) -> Dict[str, Any]:
    """
    Structural decision intelligence pipeline.
    """
    if not selected_contexts:
        raise PipelineError("At least one context must be selected.")

    # --------------------------------------------------
    # Load
    # --------------------------------------------------
    df = load_sentences_with_context()
    if df.empty:
        # Return empty structure (UI stays consistent and renders unified section with placeholder)
        return {
            "chains_by_context": {},
            "metrics": {},
            "debt": {},
            "completeness": {},
            "total_chains": 0,
            "unified_decision": {
                "action": "No data available",
                "scorecard": {"note": "Data lake is empty."},
                "alternatives": []
            },
            "action_rankings": {},
            "scoring_weights": {},
        }

    # --------------------------------------------------
    # Filter
    # --------------------------------------------------
    df = df[df["context"].isin(selected_contexts)]
    if df.empty:
        return {
            "chains_by_context": {},
            "metrics": {},
            "debt": {},
            "completeness": {},
            "total_chains": 0,
            "unified_decision": {
                "action": "No data for selected contexts",
                "scorecard": {"note": "No sentences matched the chosen contexts."},
                "alternatives": []
            },
            "action_rankings": {},
            "scoring_weights": {},
        }

    # --------------------------------------------------
    # Classify
    # --------------------------------------------------
    df = df.copy()
    df["role"] = df["sentence_text"].apply(classify_sentence)

    # --------------------------------------------------
    # Build chains per context
    # --------------------------------------------------
    chains_by_context: Dict[str, List[Dict[str, Any]]] = {}
    all_chains: List[Dict[str, Any]] = []

    for ctx in selected_contexts:
        ctx_df = df[df["context"] == ctx]
        sentences_with_roles = list(zip(ctx_df["sentence_text"], ctx_df["role"]))
        chains = build_chains(sentences_with_roles)  # Expect dicts with keys like "Cause","Action","Outcome"
        chains_by_context[ctx] = chains
        all_chains.extend(chains)

    # --------------------------------------------------
    # Decision metrics & debt (robust to edge cases)
    # --------------------------------------------------
    try:
        metrics = decision_coverage(all_chains)
    except Exception:
        metrics = {"action_coverage": 0.0, "outcome_coverage": 0.0}

    try:
        debt = analyze_chains(all_chains)
    except Exception:
        debt = {"broken_chains": 0, "cause_only": 0, "action_only": 0, "decision_debt": 0.0}

    try:
        completeness = chain_completeness(all_chains)
    except Exception:
        completeness = {}

    # Base result (keys included regardless of unified outcome)
    result: Dict[str, Any] = {
        "chains_by_context": chains_by_context,
        "metrics": metrics,
        "debt": debt,
        "completeness": completeness,
        "total_chains": len(all_chains),
        "unified_decision": None,       # will be filled below
        "action_rankings": {},          # will be filled below
        "scoring_weights": {},          # will be filled below
    }

    # --------------------------------------------------
    # Unified decision (deterministic; robust role extraction)
    # --------------------------------------------------
    if all_chains:
        weights = {
            "w1": 0.40, "w2": 0.20, "w3": 0.20,
            "w4": 0.10, "w5": 0.15, "w6": 0.25,
        }

        normalized: List[Chain] = []
        role_key_counter: Counter[str] = Counter()
        action_presence = 0

        for ctx, ctx_chains in chains_by_context.items():
            for c in ctx_chains:
                if isinstance(c, dict):
                    role_key_counter.update(k.lower() for k in c.keys())

                cause, action, outcome, ts, cons = _extract_roles(c)
                if action:
                    action_presence += 1

                normalized.append(Chain(
                    context=ctx,
                    cause=cause,
                    action=action,
                    outcome=outcome,
                    timestamp=ts,
                    constraints=cons,
                ))

        scored = _score_actions(normalized, weights)
        unified = _pick_unified(scored)

        if not unified:
            # Fallback placeholder ensures UI renders and provides diagnostics
            unified = {
                "action": "No actionable recommendation",
                "scorecard": {
                    "note": "No actions could be confidently extracted for scoring.",
                    "observed_role_keys_top5": role_key_counter.most_common(5),
                    "chains_seen": len(normalized),
                    "chains_with_action": action_presence
                },
                "alternatives": []
            }

        result["unified_decision"] = unified
        result["action_rankings"] = scored
        result["scoring_weights"] = weights
    else:
        # No chains at all; provide a placeholder unified decision
        result["unified_decision"] = {
            "action": "No actionable recommendation",
            "scorecard": {"note": "No cause/action/outcome chains detected."},
            "alternatives": []
        }
        result["action_rankings"] = {}
        result["scoring_weights"] = {}
    return result

