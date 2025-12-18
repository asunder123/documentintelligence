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
