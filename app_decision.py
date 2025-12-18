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
# Unified Decision (deterministic, orchestrator-provided)
# ============================================================

st.subheader("🟢 Unified Decision (Deterministic)")

# Read optional keys from orchestrator result without changing prior code
unified = result.get("unified_decision")
weights = result.get("scoring_weights", {})
rankings = result.get("action_rankings", {})

if not unified:
    st.info(
        "No unified recommendation available from the orchestrator.\n\n"
        "Tip: Extend `run_decision_pipeline` to add `unified_decision`, "
        "`action_rankings`, and `scoring_weights` to the result payload."
    )
else:
    # Compact scorecard view + rationale
    sc = unified.get("scorecard", {}) or {}

    colA, colB = st.columns([2, 1])
    with colA:
        st.markdown("### Recommended Action")
        st.markdown(f"**{unified.get('action', '(missing)')}**")
        st.caption(
            "Selected using structural signals (support, coverage, outcomes, constraints, debt). "
            "No inference."
        )

        st.markdown("**Rationale (scorecard)**")
        st.json({
            "score": sc.get("score"),
            "support (contexts)": sc.get("support"),
            "cause_coverage": sc.get("cause_coverage"),
            "outcome_strength": sc.get("outcome_strength"),
            "recency": sc.get("recency"),
            "constraint_fit": sc.get("constraint_fit"),
            "debt_penalty": sc.get("debt_penalty"),
            "contexts": sc.get("contexts"),
        })

    with colB:
        st.markdown("**Weights**")
        st.json(weights or {"info": "Weights not provided by orchestrator"})

    # Evidence from sample chains for auditability
    with st.expander("🔍 Evidence (sample chains)"):
        for ch in (sc.get("sample_chains") or []):
            ctx = ch.get("context")
            cause = ch.get("cause")
            action = ch.get("action")
            outcome = ch.get("outcome")
            ts = ch.get("timestamp")
            st.write(
                f"- **[{ctx}]** Cause: {cause} → **Action:** {action} → Outcome: {outcome} "
                f"{f'(ts: {ts})' if ts else ''}"
            )

    # Top alternatives (if orchestrator provided them)
    alts = unified.get("alternatives") or []
    if alts:
        st.markdown("#### Alternatives")
        for alt in alts:
            a = alt.get("action", "")
            d = alt.get("scorecard", {}) or {}
            st.write(
                f"- **{a}** "
                f"(score: {d.get('score')}, outcome_strength: {d.get('outcome_strength')}, "
                f"debt: {d.get('debt_penalty')})"
            )

# Optional: export action rankings (if available) for governance reviews
if rankings:
    import json
    dl_col, cap_col = st.columns([1, 3])
    with dl_col:
        st.download_button(
            label="⬇️ Download action rankings (JSON)",
            data=json.dumps(rankings, indent=2),
            file_name="action_rankings.json",
            mime="application/json",
            use_container_width=True
               )
    with cap_col:
        st.caption(
            "Export provides the deterministic scorecards for each action across contexts.")


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
