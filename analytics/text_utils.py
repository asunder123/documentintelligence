# analytics/text_utils.py

import re
from config import STOPWORDS


def tokenize(text: str):
    words = re.findall(r"[a-zA-Z]{3,}", text.lower())
    return [w for w in words if w not in STOPWORDS]
