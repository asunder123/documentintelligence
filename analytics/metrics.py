# analytics/metrics.py
# Cross-context analytics logic (NO UI, NO DB)
# Purely descriptive, decision-safe metrics

from collections import Counter
import pandas as pd
from analytics.text_utils import tokenize


# ------------------------------------------------------------
# Basic term analytics
# ------------------------------------------------------------

def top_terms(df, limit=20):
    tokens = []
    for text in df["sentence_text"]:
        tokens.extend(tokenize(text))
    return Counter(tokens).most_common(limit)


def top_terms_by_context(df, context, limit=10):
    ctx_df = df[df["context"] == context]
    tokens = []
    for text in ctx_df["sentence_text"]:
        tokens.extend(tokenize(text))
    return Counter(tokens).most_common(limit)


def knowledge_density(df):
    return (
        df.groupby("context")
          .size()
          .reset_index(name="Sentence Count")
          .sort_values("Sentence Count", ascending=False)
    )


# ------------------------------------------------------------
# Operational intelligence extensions (NEW)
# ------------------------------------------------------------

ISSUE_TERMS = {
    "error", "failure", "failed", "timeout", "exception",
    "latency", "crash", "authentication", "unauthorized",
    "unavailable", "degraded", "issue"
}

FIX_TERMS = {
    "restart", "rollback", "increase", "update",
    "patch", "disable", "enable", "retry", "scale"
}


def issue_density(df):
    """
    Count sentences containing issue-related signals.
    """
    issue_sentences = 0
    for text in df["sentence_text"]:
        tokens = set(tokenize(text))
        if tokens & ISSUE_TERMS:
            issue_sentences += 1

    return issue_sentences, len(df)


def fix_density(df):
    """
    Count sentences containing fix / mitigation signals.
    """
    fix_sentences = 0
    for text in df["sentence_text"]:
        tokens = set(tokenize(text))
        if tokens & FIX_TERMS:
            fix_sentences += 1

    return fix_sentences, len(df)


def issue_fix_pairs(df):
    """
    Detect sentences where problem and fix terms co-occur.
    These are strong RCA / runbook signals.
    """
    pairs = []
    for text in df["sentence_text"]:
        tokens = set(tokenize(text))
        if tokens & ISSUE_TERMS and tokens & FIX_TERMS:
            pairs.append(text)

    return pairs


def context_maturity(df):
    """
    Simple documentation maturity heuristic.
    Higher = richer, more varied language.
    """
    all_tokens = []
    for text in df["sentence_text"]:
        all_tokens.extend(tokenize(text))

    if not all_tokens:
        return 0.0

    return len(set(all_tokens)) / len(all_tokens)
