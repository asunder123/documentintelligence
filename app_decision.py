# app_decision.py
# Decision Intelligence View (Multi-Context)
# Structural, rule-based, explainable

import streamlit as st
import pandas as pd

from analytics.loader import load_sentences_with_context
from decision.role_classifier import classify_sentence
from decision.chain_builder import build_chains
from decision.metrics import decision_coverage


# ============================================================
# Page setup
# ============================================================

st.set_page_config(page_title="Decision Intelligence", layout="wide")
st.title("🧭 Decision Intelligence")

st.caption(
    "Analyzes how problems, causes, actions, and outcomes are documented "
    "across one or more contexts. Structural, explainable, non-LLM."
)


# ============================================================
# Load data
# ============================================================

df = load_sentences_with_context()

if df.empty:
    st.warning("No data available. Ingest documents first.")
    st.stop()


# ============================================================
# Context selection (MULTI-SELECT)
# ============================================================

all_contexts = sorted(df["context"].unique())

selected_contexts = st.multiselect(
    "Select context(s) for decision analysis",
    all_contexts,
    default=all_contexts[:1]
)

if not selected_contexts:
    st.info("Select at least one context to proceed.")
    st.stop()


df = df[df["context"].isin(selected_contexts)]


# ============================================================
# Role classification (per sentence)
# ============================================================

df["role"] = df["sentence_text"].apply(classify_sentence)


# ============================================================
# Build chains PER CONTEXT
# ============================================================

all_chains = []
chains_by_context = {}

for ctx in selected_contexts:
    ctx_df = df[df["context"] == ctx]

    sentences_with_roles = list(
        zip(ctx_df["sentence_text"], ctx_df["role"])
    )

    chains = build_chains(sentences_with_roles)

    chains_by_context[ctx] = chains
    all_chains.extend(chains)


# ============================================================
# Aggregate decision metrics
# ============================================================

overall_metrics = decision_coverage(all_chains)


st.subheader("📊 Overall Decision Coverage")

col1, col2, col3 = st.columns(3)
col1.metric("Total cause chains", overall_metrics.get("total_chains", 0))
col2.metric(
    "Action coverage",
    f"{overall_metrics.get('action_coverage', 0) * 100:.1f}%"
)
col3.metric(
    "Outcome coverage",
    f"{overall_metrics.get('outcome_coverage', 0) * 100:.1f}%"
)


# ============================================================
# Context-wise comparison
# ============================================================

st.subheader("📂 Context-wise Decision Maturity")

rows = []

for ctx, chains in chains_by_context.items():
    m = decision_coverage(chains)
    rows.append({
        "Context": ctx,
        "Cause chains": m.get("total_chains", 0),
        "Action coverage (%)": round(m.get("action_coverage", 0) * 100, 1),
        "Outcome coverage (%)": round(m.get("outcome_coverage", 0) * 100, 1),
    })

comparison_df = pd.DataFrame(rows)
st.dataframe(comparison_df, use_container_width=True)


# ============================================================
# Sample chains (cross-context)
# ============================================================

st.subheader("🔗 Sample Cause → Action → Outcome Chains")

shown = 0
MAX_SHOW = 5

for ctx, chains in chains_by_context.items():
    for c in chains:
        if shown >= MAX_SHOW:
            break

        with st.expander(f"Context: {ctx}"):
            for k, v in c.items():
                st.write(f"**{k}**: {v}")

        shown += 1

    if shown >= MAX_SHOW:
        break


# ============================================================
# Interpretation helper (SAFE, NON-INFERENTIAL)
# ============================================================

st.subheader("🧠 How to read this")

st.markdown(
    """
- **Cause chains** indicate where issues are *explained*, not just observed  
- **Action coverage** shows how often causes lead to mitigations  
- **Outcome coverage** shows whether actions are followed by results  

Low outcome coverage usually signals:
- firefighting
- incomplete RCAs
- or undocumented learnings

This view is structural and descriptive — it does not infer intent.
"""
)
