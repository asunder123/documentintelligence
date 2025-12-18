
# app_query.py
# STEP 2: Context-aware querying over SQLite data lake
# FIXED:
# - shared TF-IDF vectorizer
# - sparse matrix handling
# - correct similarity space
# - deterministic refusal behavior

import streamlit as st
import sqlite3
import pickle
import numpy as np

from scipy.sparse import vstack  # 🔴 CRITICAL FIX
from scipy.sparse import issparse
from sklearn.metrics.pairwise import cosine_similarity
from engine import CaseIndex

DB_PATH = "storage/data_lake.db"


# ============================================================
# DB helpers
# ============================================================

def get_connection():
    return sqlite3.connect(DB_PATH)


def load_available_contexts():
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT DISTINCT context FROM documents ORDER BY context"
        ).fetchall()
        contexts = [r[0] for r in rows if r[0]]
    except Exception:
        contexts = []
    conn.close()
    return contexts


def load_sentences_and_vectors(context):
    """
    Load sentence texts and their TF-IDF vectors for ONE context.
    """
    conn = get_connection()
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT s.sentence_text, s.vector
        FROM sentences s
        JOIN documents d ON s.document_id = d.document_id
        WHERE d.context = ?
        ORDER BY d.document_id, s.sentence_index
    """, (context,)).fetchall()

    conn.close()

    sentences = []
    vectors = []

    for sentence_text, vector_blob in rows:
        sentences.append(sentence_text)
        vectors.append(pickle.loads(vector_blob))

    return sentences, vectors


def load_vectorizer(context):
    """
    Load the persisted TF-IDF vectorizer for a context.
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT vectorizer FROM vectorizers WHERE context = ?",
        (context,)
    ).fetchone()
    conn.close()

    if not row:
        return None

    return pickle.loads(row[0])


# ============================================================
# UI setup
# ============================================================

st.set_page_config(page_title="Context-Aware Document Query", layout="wide")
st.title("🔎 Context-Aware Document Query")

st.caption(
    "Evidence-based answers derived strictly from document similarity. "
    "If evidence is weak, the system will refuse to answer."
)


# ============================================================
# Context selection
# ============================================================

contexts = load_available_contexts()

if not contexts:
    st.warning("No contexts found. Run the ingestion app first.")
    st.stop()

selected_contexts = st.multiselect(
    "Select context to query",
    contexts
)

# 🔒 Enforce single-context querying for correctness
if len(selected_contexts) != 1:
    st.info(
        "Select exactly ONE context.\n\n"
        "Vectorizers are trained per context to preserve semantic integrity."
    )
    st.stop()

context = selected_contexts[0]


# ============================================================
# Load data into engine
# ============================================================

with st.spinner("Loading context data…"):
    sentences, vectors = load_sentences_and_vectors(context)
    vectorizer = load_vectorizer(context)

if not sentences:
    st.error("No sentences found for this context.")
    st.stop()

if vectorizer is None:
    st.error("No vectorizer found for this context. Re-ingest documents.")
    st.stop()

# 🔴 Build in-memory index correctly
index = CaseIndex()
index.sentences = sentences
index.matrix = vstack(vectors)     # ✅ sparse-safe
index.vectorizer = vectorizer      # ✅ shared TF-IDF space

st.success(f"Loaded {len(sentences)} sentences from context '{context}'")


# ============================================================
# Query interface
# ============================================================

# Minimal additions to keep most code same
top_k = st.number_input("Top K matches", min_value=1, max_value=500, value=50, step=1)
min_score = st.slider("Min similarity score", 0.0, 1.0, 0.35, 0.01)

query = st.text_input(
    "Ask a question",
    placeholder="e.g. What caused authentication failures?"
)

# ---------- Local fallback: rank ALL sentences if CaseIndex returns only top-1 ----------
def _fallback_rank_all(q_text: str, k: int, threshold: float):
    """
    Compute cosine similarity between query TF-IDF vector and all sentence vectors,
    and return top-k matches with a minimal result schema.
    Keeps your UI compatible even if CaseIndex.query returns only top-1.
    """
    if not q_text or not index.vectorizer or index.matrix is None:
        return []

    # Vectorize query in the SAME TF-IDF space
    try:
        q_vec = index.vectorizer.transform([q_text])
    except Exception:
        return []

    # Ensure sparse formats; cosine_similarity handles CSR efficiently
    A = index.matrix
    B = q_vec
    if not issparse(A):
        # Should not happen since we vstack’d sparse vectors
        A = vstack([A])
    # Cosine similarity (1 x N)
    try:
        sims = cosine_similarity(B, A)  # shape: (1, N)
        scores = sims.ravel()
    except Exception:
        # Fallback to dot if vectors are L2-normalized TF-IDF
        scores = (A @ B.T).toarray().ravel()

    # Rank + filter
    order = np.argsort(-scores)  # descending
    results = []
    added = 0
    for idx in order:
        sc = float(scores[idx])
        if sc < float(threshold):
            # Remaining tail will be smaller—stop early
            if added > 0:
                break
            else:
                continue
        sent = index.sentences[idx]
        # Minimal result structure (UI-compatible)
        results.append({
            "score": sc,
            "answer": sent,           # keep super simple: the sentence itself
            "evidence": sent,         # same sentence as verbatim evidence
            "stitched_count": 1       # we did not stitch in fallback mode
        })
        added += 1
        if added >= int(k):
            break
    return results
# ---------------------------------------------------------------------------------------

if query:
    # First try native CaseIndex parameters; if it still returns a single record,
    # we augment with the fallback to ensure multiple matches.
    use_native = True
    try:
        results = index.query(query, top_k=int(top_k), score_threshold=float(min_score))
        # Defensive: if result is not a list or has length <= 1 but k > 1, use fallback
        if not isinstance(results, list) or (len(results) <= 1 and top_k > 1):
            use_native = False
    except TypeError:
        # Engine signature doesn’t accept our params
        use_native = False
    except Exception:
        # Engine error—fallback
        use_native = False

    if not use_native:
        results = _fallback_rank_all(query, top_k, min_score)

    if not results:
        st.warning(
            "No sufficiently similar evidence found.\n\n"
            "This is a correct refusal based on document content."
        )
    else:
        st.subheader("Answers (Evidence-Based)")

        # ⬇️ Show ALL matches — one block per result
        for i, r in enumerate(results, start=1):
            st.markdown(f"**Match {i}**")

            # Answer
            st.write(r.get("answer", ""))

            # Meta caption
            score = float(r.get("score", 0.0))
            stitched = int(r.get("stitched_count", 0))
            st.caption(
                f"Similarity score: {score:.4f} | "
                f"Sentences stitched: {stitched}"
            )

            # Evidence — one expander per match (no nesting)
            evidence = r.get("evidence", "")
            if evidence:
                with st.expander("🔎 Evidence (verbatim from documents)"):
                    st.write(evidence)

