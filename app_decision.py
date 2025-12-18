# app_decision.py
# Decision Intelligence UI
# Fully orchestrator-driven (NO backend logic here)

import streamlit as st
import pandas as pd

from analytics.loader import load_available_contexts
from orchestration.pipeline import (
    run_decision_pipeline,
    PipelineError
)


# ============================================================
# Page setup
# ============================================================

st.set_page_config(page_title="Decision Intelligence", layout="wide")
st.title("🧭 Decision Intelligence")

st.caption(
    "Structural, rule-based decision intelligence derived from documents. "
    "This view analyzes how problems, causes, actions, outcomes, and constraints "
    "are documented across one or more contexts. "
    "No inference, no LLMs, fully explainable."
)


# ============================================================
# Context selection
# ============================================================

contexts = load_available_contexts()

if not contexts:
    st.warning("No contexts available. Ingest documents first.")
    st.stop()

selected_contexts = st.multiselect(
    "Select context(s) for decision analysis",
    contexts,
    default=contexts[:1]
)

if not selected_contexts:
    st.info("Select at least one context to proceed.")
    st.stop()


# ============================================================
# Run decision pipeline (UNIFIED ORCHESTRATOR)
# ============================================================

with st.spinner("Running decision intelligence pipeline…"):
    try:
        result = run_decision_pipeline(selected_contexts)
    except PipelineError as e:
        st.error(str(e))
        st.stop()


chains_by_context = result["chains_by_context"]
metrics = result["metrics"]
debt = result["debt"]
completeness = result["completeness"]
total_chains = result["total_chains"]


# ============================================================
# High-level decision coverage
# ============================================================

st.subheader("📊 Decision Coverage (Across Selected Contexts)")

col1, col2, col3 = st.columns(3)

col1.metric(
    "Total cause chains",
    total_chains,
    help="Distinct cause-led decision chains detected"
)

col2.metric(
    "Action coverage",
    f"{metrics.get('action_coverage', 0) * 100:.1f}%",
    help="How often documented causes lead to actions"
)

col3.metric(
    "Outcome coverage",
    f"{metrics.get('outcome_coverage', 0) * 100:.1f}%",
    help="How often actions document outcomes"
)


# ============================================================
# Decision debt & broken chains
# ============================================================

st.subheader("⚠️ Decision Debt & Broken Chains")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Broken chains",
    debt.get("broken_chains", 0),
    help="Chains missing actions or outcomes"
)

col2.metric(
    "Cause only",
    debt.get("cause_only", 0),
    help="Known issues without documented action"
)

col3.metric(
    "Action only",
    debt.get("action_only", 0),
    help="Actions taken without stated causes"
)

col4.metric(
    "Decision debt",
    f"{debt.get('decision_debt', 0) * 100:.1f}%",
    help="Higher = more unresolved or incomplete decisions"
)

st.caption(
    "Decision debt highlights where issues are identified but not fully "
    "resolved or documented. This is a structural signal, not a judgement."
)


# ============================================================
# Context-wise comparison
# ============================================================

st.subheader("📂 Context-wise Decision Maturity")

rows = []

for ctx, chains in chains_by_context.items():
    from decision.metrics import decision_coverage  # read-only

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
# Sample decision chains
# ============================================================

st.subheader("🔗 Sample Cause → Action → Outcome Chains")

MAX_SHOW = 6
shown = 0

for ctx, chains in chains_by_context.items():
    for chain in chains:
        if shown >= MAX_SHOW:
            break

        with st.expander(f"Context: {ctx}"):
            for role, sentence in chain.items():
                st.write(f"**{role}**: {sentence}")

        shown += 1

    if shown >= MAX_SHOW:
        break


# ============================================================
# Interpretation guide (static, safe)
# ============================================================

st.subheader("🧠 How to interpret this view")

st.markdown(
    """
**What this shows**
- **Cause chains** indicate explained issues
- **Action coverage** shows whether explanations lead to mitigation
- **Outcome coverage** shows whether learning is closed
- **Decision debt** highlights unresolved or undocumented decisions

**What this does NOT do**
- It does not judge correctness
- It does not infer intent
- It does not predict outcomes

This view is **structural, deterministic, and auditable**.
"""
)
