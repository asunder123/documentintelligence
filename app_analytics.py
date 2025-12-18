# app_analytics.py
# STEP 3: Cross-context analytics (decision-oriented, read-only)

import streamlit as st
import pandas as pd

from analytics.loader import load_sentences_with_context, load_available_contexts
from analytics.metrics import (
    top_terms,
    top_terms_by_context,
    knowledge_density,
    issue_density,
    fix_density,
    issue_fix_pairs,
    context_maturity
)


# ============================================================
# UI setup
# ============================================================

st.set_page_config(page_title="Cross-Context Analytics", layout="wide")
st.title("📊 Cross-Context Document Intelligence")

st.caption(
    "This view surfaces operational patterns, risks, and documentation health "
    "directly from stored documents. No querying, no inference, no LLMs."
)


# ============================================================
# Load data
# ============================================================

df = load_sentences_with_context()

if df.empty:
    st.warning("No data found. Run the ingestion app first.")
    st.stop()


# ============================================================
# Context selection
# ============================================================

contexts = load_available_contexts()

selected_contexts = st.multiselect(
    "Select context(s) to analyze",
    contexts,
    default=contexts
)

df = df[df["context"].isin(selected_contexts)]

if df.empty:
    st.warning("No sentences found for selected contexts.")
    st.stop()


# ============================================================
# High-level metrics
# ============================================================

st.subheader("📌 High-level metrics")

col1, col2, col3 = st.columns(3)
col1.metric("Contexts analyzed", len(selected_contexts))
col2.metric("Total sentences", len(df))
col3.metric("Distinct sentences", df["sentence_text"].nunique())


# ============================================================
# Operational signals (NEW)
# ============================================================

st.subheader("🚨 Operational signals")

issues, total = issue_density(df)
fixes, _ = fix_density(df)

col1, col2, col3 = st.columns(3)

col1.metric(
    "Issue-heavy sentences",
    issues,
    f"{(issues / total) * 100:.1f}% of total"
)

col2.metric(
    "Fix-related sentences",
    fixes,
    f"{(fixes / total) * 100:.1f}% of total"
)

col3.metric(
    "Knowledge maturity score",
    f"{context_maturity(df):.3f}",
    help="Higher = richer, more varied documentation"
)


# ============================================================
# Most common terms (overall)
# ============================================================

st.subheader("🔑 Most common terms (overall)")

terms = top_terms(df, limit=20)
terms_df = pd.DataFrame(terms, columns=["Term", "Frequency"])
st.dataframe(terms_df, use_container_width=True)


# ============================================================
# Top terms by context
# ============================================================

st.subheader("📂 Top terms by context")

for ctx in selected_contexts:
    ctx_terms = top_terms_by_context(df, ctx, limit=10)
    if not ctx_terms:
        continue

    ctx_df = pd.DataFrame(ctx_terms, columns=["Term", "Frequency"])
    with st.expander(f"Context: {ctx}"):
        st.dataframe(ctx_df, use_container_width=True)


# ============================================================
# Recurring problem → fix patterns (NEW)
# ============================================================

st.subheader("🔁 Recurring problem → fix patterns")

pairs = issue_fix_pairs(df)

if not pairs:
    st.info("No clear problem–fix co-occurrence patterns detected.")
else:
    st.write(
        f"Detected {len(pairs)} sentences where issues and fixes appear together."
    )

    for p in pairs[:5]:
        with st.expander("Example pattern"):
            st.write(p)


# ============================================================
# Knowledge density by context
# ============================================================

st.subheader("🧠 Knowledge density by context")

density_df = knowledge_density(df)
st.bar_chart(density_df.set_index("context"))
