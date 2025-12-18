# decision/role_classifier.py
# Rule-based sentence role classification (NO ML)

import re

ROLE_RULES = {
    "CAUSE": [
        r"\bbecause\b",
        r"\bdue to\b",
        r"\bas a result of\b",
        r"\broot cause\b"
    ],
    "ACTION": [
        r"\brestart(ed)?\b",
        r"\brollback\b",
        r"\bfix(ed)?\b",
        r"\bpatch(ed)?\b",
        r"\bupdate(d)?\b",
        r"\bdisable(d)?\b",
        r"\bretry\b"
    ],
    "OUTCOME": [
        r"\bresolved\b",
        r"\brecovered\b",
        r"\bstable\b",
        r"\bsuccessfully\b",
        r"\bfixed\b"
    ],
    "CONSTRAINT": [
        r"\bcannot\b",
        r"\blimitation\b",
        r"\brisk\b",
        r"\bnot possible\b",
        r"\btrade[- ]off\b"
    ],
    "PROBLEM": [
        r"\berror\b",
        r"\bfailure\b",
        r"\btimeout\b",
        r"\bincident\b",
        r"\bunavailable\b",
        r"\bdegraded\b"
    ]
}


def classify_sentence(sentence: str) -> str:
    s = sentence.lower()

    for role, patterns in ROLE_RULES.items():
        for p in patterns:
            if re.search(p, s):
                return role

    return "OBSERVATION"
