# orchestration/pipeline.py
# Unified deterministic orchestrator for the document intelligence system

from typing import List, Dict, Any

from analytics.loader import load_sentences_with_context
from decision.role_classifier import classify_sentence
from decision.chain_builder import build_chains
from decision.metrics import decision_coverage
from engine import CaseIndex

import sqlite3
import pickle
from scipy.sparse import vstack

from decision.debt import analyze_chains
from decision.metrics import chain_completeness


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
        (context,)
    ).fetchone()

    if not row:
        conn.close()
        raise PipelineError("Vectorizer not found for context.")

    vectorizer = pickle.loads(row[0])

    # --------------------------------------------------
    # Load sentences & vectors
    # --------------------------------------------------
    rows = cur.execute("""
        SELECT s.sentence_text, s.vector
        FROM sentences s
        JOIN documents d ON s.document_id = d.document_id
        WHERE d.context = ?
        ORDER BY d.document_id, s.sentence_index
    """, (context,)).fetchall()

    conn.close()

    if not rows:
        raise PipelineError("No sentences found for context.")

    sentences = []
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
        "total_sentences": len(sentences)
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
        raise PipelineError("No data in data lake.")

    # --------------------------------------------------
    # Filter
    # --------------------------------------------------
    df = df[df["context"].isin(selected_contexts)]
    if df.empty:
        raise PipelineError("No data for selected contexts.")

    # --------------------------------------------------
    # Classify
    # --------------------------------------------------
    df = df.copy()
    df["role"] = df["sentence_text"].apply(classify_sentence)

    # --------------------------------------------------
    # Build chains per context
    # --------------------------------------------------
    chains_by_context = {}
    all_chains = []

    for ctx in selected_contexts:
        ctx_df = df[df["context"] == ctx]

        sentences_with_roles = list(
            zip(ctx_df["sentence_text"], ctx_df["role"])
        )

        chains = build_chains(sentences_with_roles)
        chains_by_context[ctx] = chains
        all_chains.extend(chains)

    # --------------------------------------------------
   # Decision metrics & debt
   # --------------------------------------------------
        metrics = decision_coverage(all_chains)
        debt = analyze_chains(all_chains)
        completeness = chain_completeness(all_chains)

        return {
        "chains_by_context": chains_by_context,
        "metrics": metrics,
        "debt": debt,
        "completeness": completeness,
        "total_chains": len(all_chains)
         }
