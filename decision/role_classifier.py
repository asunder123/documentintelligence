
# decision/role_classifier.py
# Rule-based sentence role classification (NO ML)

import re
from typing import Dict, List

ROLE_RULES: Dict[str, List[str]] = {
    "CAUSE": [
        r"\bbecause\b",
        r"\bdue to\b",
        r"\bas a result of\b",
        r"\broot cause\b",
    ],
    "ACTION": [
        r"\brestart(ed)?\b",
        r"\brollback\b",
        r"\bfix(ed)?\b",
        r"\bpatch(ed)?\b",
        r"\bupdate(d)?\b",
        r"\bdisable(d)?\b",
        r"\bretry\b",
    ],
    "OUTCOME": [
        r"\bresolved\b",
        r"\brecovered\b",
        r"\bstable\b",
        r"\bsuccessfully\b",
        r"\bfixed\b",
    ],
    "CONSTRAINT": [
        r"\bcannot\b",
        r"\blimitation\b",
        r"\brisk\b",
        r"\bnot possible\b",
        r"\btrade[- ]off\b",
    ],
    "PROBLEM": [
        r"\berror\b",
        r"\bfailure\b",
        r"\btimeout\b",
        r"\bincident\b",
        r"\bunavailable\b",
        r"\bdegraded\b",
        r"\bunallocated\b",   # <-- FIXED: was r"\unallocated\b" (bad \u escape)
    ],
}

# Precompile with IGNORECASE for performance and robustness
_COMPILED_RULES: Dict[str, List[re.Pattern]] = {
    role: [re.compile(pat, flags=re.IGNORECASE) for pat in patterns]
    for role, patterns in ROLE_RULES.items()
}

def classify_sentence(sentence: str) -> str:
    s = sentence.strip()
    if not s:
        return "OBSERVATION"

    for role, patterns in _COMPILED_RULES.items():
        for pat in patterns:
            try:
                if pat.search(s):
                    return role
            except re.error as e:
                # Skip unexpected bad patterns without crashing the pipeline
                # (shouldn't happen since we precompiled, but guard anyway)
                # You can log this if you have a logger:
                # print(f"[role_classifier] Bad regex {pat.pattern!r}: {e}")
                continue

   
