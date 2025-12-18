# decision/metrics.py
# Decision intelligence metrics

def decision_coverage(chains):
    total = len(chains)
    if total == 0:
        return {}

    with_action = sum(1 for c in chains if "ACTION" in c)
    with_outcome = sum(1 for c in chains if "OUTCOME" in c)

    return {
        "total_chains": total,
        "action_coverage": with_action / total,
        "outcome_coverage": with_outcome / total
    }


# decision/metrics.py (append)

def chain_completeness(chains):
    """
    Return completeness ratios for reporting.
    """
    if not chains:
        return {}

    complete = sum(
        1 for c in chains
        if "CAUSE" in c and "ACTION" in c and "OUTCOME" in c
    )

    return {
        "complete_ratio": complete / len(chains),
        "incomplete_ratio": 1 - (complete / len(chains))
    }
