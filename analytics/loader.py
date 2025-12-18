# analytics/loader.py
# Safe SQLite loader with schema initialization
# Read-only analytics, but schema-aware

import sqlite3
import pandas as pd
from config import DB_PATH


# ============================================================
# Schema initialization (idempotent)
# ============================================================

def init_schema(conn: sqlite3.Connection):
    """
    Ensure required tables exist.
    This prevents 'no such table' runtime errors.
    """
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

    conn.commit()


# ============================================================
# Connection helper
# ============================================================

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    init_schema(conn)
    return conn


# ============================================================
# Loaders
# ============================================================

def load_sentences_with_context():
    """
    Load all sentences joined with their contexts.
    Safe even if DB was partially initialized.
    """
    conn = get_connection()

    try:
        df = pd.read_sql_query("""
            SELECT d.context, s.sentence_text
            FROM sentences s
            JOIN documents d ON s.document_id = d.document_id
        """, conn)
    except Exception:
        # Table exists but empty or incompatible
        df = pd.DataFrame(columns=["context", "sentence_text"])

    conn.close()
    return df


def load_available_contexts():
    """
    Return distinct contexts available in the data lake.
    """
    conn = get_connection()

    try:
        rows = conn.execute(
            "SELECT DISTINCT context FROM documents ORDER BY context"
        ).fetchall()
        contexts = [r[0] for r in rows if r[0] is not None]
    except Exception:
        contexts = []

    conn.close()
    return contexts
