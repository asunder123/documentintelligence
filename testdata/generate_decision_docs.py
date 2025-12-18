# generate_decision_docs.py
# Bulk document generator for Decision Intelligence testing
# Produces structurally meaningful .txt files

import os
import random
from datetime import datetime

OUTPUT_DIR = "generated_docs"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# Sentence components
# ============================================================

PROBLEMS = [
    "The service experienced intermittent authentication failures.",
    "Several requests timed out during peak traffic.",
    "The application became unavailable for multiple users.",
    "Latency increased significantly across the API.",
    "Transactions were rejected by the gateway."
]

CAUSES = [
    "The root cause was an expired authentication token.",
    "This happened because the database connection pool was exhausted.",
    "The issue was caused by a misconfigured cache.",
    "The failure occurred due to a stalled background job.",
    "This was due to increased load on the service."
]

ACTIONS = [
    "Engineers restarted the affected service.",
    "The cache was cleared and rebuilt.",
    "The deployment was rolled back to the previous version.",
    "The service configuration was updated.",
    "Additional capacity was provisioned."
]

OUTCOMES = [
    "The service recovered within five minutes.",
    "Normal operations were restored shortly after.",
    "No further incidents were observed.",
    "Customer impact was mitigated successfully.",
    "The system stabilized after the fix."
]

CONSTRAINTS = [
    "The issue cannot be fixed immediately due to legacy dependencies.",
    "Upgrading the component is not possible in the current release cycle.",
    "The risk has been accepted temporarily.",
    "A permanent fix is deferred due to business constraints."
]

OBSERVATIONS = [
    "Monitoring alerts were triggered during the incident.",
    "Logs showed repeated warning messages.",
    "The incident was detected by automated checks.",
    "Metrics indicated abnormal behavior."
]


# ============================================================
# Document patterns
# ============================================================

def complete_chain():
    return [
        random.choice(PROBLEMS),
        random.choice(CAUSES),
        random.choice(ACTIONS),
        random.choice(OUTCOMES)
    ]


def cause_only():
    return [
        random.choice(PROBLEMS),
        random.choice(CAUSES),
        "The issue remains under investigation."
    ]


def cause_action_only():
    return [
        random.choice(PROBLEMS),
        random.choice(CAUSES),
        random.choice(ACTIONS)
    ]


def action_only():
    return [
        random.choice(ACTIONS),
        random.choice(OBSERVATIONS)
    ]


def constraint_only():
    return [
        random.choice(PROBLEMS),
        random.choice(CONSTRAINTS),
        random.choice(OBSERVATIONS)
    ]


PATTERNS = [
    (complete_chain, 0.40),
    (cause_only, 0.20),
    (cause_action_only, 0.15),
    (action_only, 0.15),
    (constraint_only, 0.10)
]


# ============================================================
# Context mapping
# ============================================================

CONTEXTS = {
    "Payments-RCA": ["complete_chain", "cause_only", "cause_action_only"],
    "Infra-Runbooks": ["action_only"],
    "Security-Notes": ["constraint_only", "cause_only"],
    "General-Observations": ["action_only", "constraint_only"]
}


# ============================================================
# Generator
# ============================================================

def choose_pattern():
    r = random.random()
    cumulative = 0.0
    for func, weight in PATTERNS:
        cumulative += weight
        if r <= cumulative:
            return func
    return complete_chain


def generate_document(doc_id: int, context: str):
    pattern_func = choose_pattern()
    lines = pattern_func()

    # add noise sentences
    if random.random() < 0.5:
        lines.insert(0, random.choice(OBSERVATIONS))

    timestamp = datetime.utcnow().isoformat()

    content = [
        f"Document ID: {doc_id}",
        f"Context: {context}",
        f"Generated at: {timestamp}",
        ""
    ] + lines

    return "\n".join(content)


# ============================================================
# Main
# ============================================================

def main(num_docs=200):
    doc_id = 1

    for context in CONTEXTS.keys():
        ctx_dir = os.path.join(OUTPUT_DIR, context)
        os.makedirs(ctx_dir, exist_ok=True)

        for _ in range(num_docs // len(CONTEXTS)):
            text = generate_document(doc_id, context)
            filename = f"{context.lower().replace('-', '_')}_{doc_id}.txt"

            with open(os.path.join(ctx_dir, filename), "w", encoding="utf-8") as f:
                f.write(text)

            doc_id += 1

    print(f"Generated {doc_id - 1} documents in '{OUTPUT_DIR}'")


if __name__ == "__main__":
    main(num_docs=200)
