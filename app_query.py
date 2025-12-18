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

from scipy.sparse import vstack  # 🔴 CRITICAL FIX
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

query = st.text_input(
    "Ask a question",
    placeholder="e.g. What caused authentication failures?"
)

if query:
    results = index.query(query)

    if not results:
        st.warning(
            "No sufficiently similar evidence found.\n\n"
            "This is a correct refusal based on document content."
        )
    else:
        r = results[0]

        st.subheader("Answer (Evidence-Based)")
        st.write(r["answer"])

        st.caption(
            f"Similarity score: {r['score']:.4f} | "
            f"Sentences stitched: {r['stitched_count']}"
        )

        with st.expander("🔎 Evidence (verbatim from documents)"):
            st.write(r["evidence"])
