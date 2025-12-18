# app_ingest.py
# STEP 1: Batch ingestion into SQLite data lake (FIXED)

import streamlit as st
import sqlite3
import uuid
import hashlib
from datetime import datetime
import os
import pickle

from ingestion.extractor import extract_text_from_file
from engine import extract_core_text, split_sentences
from sklearn.feature_extraction.text import TfidfVectorizer

DB_PATH = "storage/data_lake.db"
os.makedirs("storage", exist_ok=True)


# ============================================================
# DB helpers
# ============================================================

def get_connection():
    conn = sqlite3.connect(DB_PATH)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            context TEXT,
            filename TEXT,
            doc_type TEXT,
            ingested_at TEXT,
            content_hash TEXT,
            raw_text TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sentences (
            sentence_id TEXT PRIMARY KEY,
            document_id TEXT,
            sentence_index INTEGER,
            sentence_text TEXT,
            vector BLOB,
            FOREIGN KEY(document_id) REFERENCES documents(document_id)
        )
    """)

    # 🔴 CRITICAL TABLE (FIX)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vectorizers (
            context TEXT PRIMARY KEY,
            vectorizer BLOB
        )
    """)

    conn.commit()
    return conn


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ============================================================
# UI
# ============================================================

st.set_page_config(page_title="Document Data Lake Ingestion", layout="wide")
st.title("📥 Document Data Lake Ingestion")

context = st.text_input(
    "Context (required)",
    placeholder="e.g. Payments-RCA, Runbooks-Prod"
)

uploaded_files = st.file_uploader(
    "Upload documents (batch supported)",
    type=["txt", "md", "csv", "json", "html", "docx", "pdf", "xlsx"],
    accept_multiple_files=True
)

if uploaded_files:
    st.info(f"{len(uploaded_files)} file(s) selected")

if st.button("Ingest Documents"):

    if not context.strip():
        st.error("Context is required")
        st.stop()

    if not uploaded_files:
        st.error("Upload at least one document")
        st.stop()

    conn = get_connection()
    cur = conn.cursor()

    all_sentences = []

    # --------------------------------------------------------
    # First pass: extract & collect sentences
    # --------------------------------------------------------
    for uploaded in uploaded_files:
        raw_text = extract_text_from_file(uploaded)
        raw_text = extract_core_text(raw_text)

        if not raw_text.strip():
            st.warning(f"Skipped {uploaded.name}: no readable text")
            continue

        content_hash = hash_text(raw_text)

        exists = cur.execute(
            "SELECT 1 FROM documents WHERE content_hash = ?",
            (content_hash,)
        ).fetchone()

        if exists:
            st.info(f"Duplicate skipped: {uploaded.name}")
            continue

        sentences = split_sentences(raw_text)
        if not sentences:
            continue

        document_id = str(uuid.uuid4())
        ingested_at = datetime.utcnow().isoformat()

        cur.execute("""
            INSERT INTO documents VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            document_id,
            context,
            uploaded.name,
            uploaded.name.split(".")[-1],
            ingested_at,
            content_hash,
            raw_text
        ))

        for idx, s in enumerate(sentences):
            all_sentences.append((document_id, idx, s))

    if not all_sentences:
        st.warning("No valid sentences extracted.")
        conn.close()
        st.stop()

    # --------------------------------------------------------
    # Fit ONE vectorizer per context (CRITICAL FIX)
    # --------------------------------------------------------
    sentence_texts = [s[2] for s in all_sentences]

    vectorizer = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(3, 5),
        min_df=1
    )

    matrix = vectorizer.fit_transform(sentence_texts)

    # Store vectorizer
    cur.execute("""
        INSERT OR REPLACE INTO vectorizers VALUES (?, ?)
    """, (context, pickle.dumps(vectorizer)))

    # Store sentence vectors
    for i, (doc_id, idx, sentence) in enumerate(all_sentences):
        cur.execute("""
            INSERT INTO sentences VALUES (?, ?, ?, ?, ?)
        """, (
            str(uuid.uuid4()),
            doc_id,
            idx,
            sentence,
            pickle.dumps(matrix[i])
        ))

    conn.commit()
    conn.close()

    st.success("Batch ingestion completed successfully")
