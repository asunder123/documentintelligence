# decision/debt.py
# Decision debt and broken-chain analysis

def analyze_chains(chains):
    """
    Analyze decision chains for completeness and debt.

    Returns:
    {
      "broken_chains": int,
      "cause_only": int,
      "action_only": int,
      "cause_action_only": int,
      "complete": int,
      "decision_debt": float
    }
    """

    stats = {
        "broken_chains": 0,
        "cause_only": 0,
        "action_only": 0,
        "cause_action_only": 0,
        "complete": 0,
    }

    for c in chains:
        has_cause = "CAUSE" in c
        has_action = "ACTION" in c
        has_outcome = "OUTCOME" in c

        if has_cause and has_action and has_outcome:
            stats["complete"] += 1
        elif has_cause and not has_action:
            stats["cause_only"] += 1
            stats["broken_chains"] += 1
        elif has_action and not has_outcome:
            stats["action_only"] += 1
            stats["broken_chains"] += 1
        elif has_cause and has_action and not has_outcome:
            stats["cause_action_only"] += 1
            stats["broken_chains"] += 1
        else:
            stats["broken_chains"] += 1

    total = len(chains)
    stats["decision_debt"] = (
        stats["broken_chains"] / total if total else 0.0
    )

    return stats
