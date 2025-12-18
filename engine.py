# engine.py
# Core document intelligence engine
# Responsibility:
# - text cleanup
# - sentence segmentation
# - similarity-based querying
# - evidence stitching
# NO persistence, NO UI, NO LLM

import re
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# ============================================================
# Text cleanup (structural intelligence – light)
# ============================================================

def extract_core_text(text: str) -> str:
    """
    Remove obvious noise without destroying meaning.
    Keeps this conservative.
    """
    if not text:
        return ""

    # Remove repeated whitespace
    text = re.sub(r"\s+", " ", text)

    # Remove very long non-word sequences (binary / noise)
    text = re.sub(r"[^\w\s]{10,}", " ", text)

    return text.strip()


# ============================================================
# Sentence segmentation
# ============================================================

def split_sentences(text: str):
    """
    Simple, deterministic sentence splitter.
    No NLP dependency to keep it robust.
    """
    if not text:
        return []

    # Split on sentence boundaries
    sentences = re.split(r"(?<=[.!?])\s+", text)

    # Clean and filter
    clean = []
    for s in sentences:
        s = s.strip()
        if len(s) >= 20:          # drop trivial fragments
            clean.append(s)

    return clean


# ============================================================
# Core index
# ============================================================

class CaseIndex:
    """
    In-memory sentence index.
    Can be built from text OR populated from storage.
    """

    def __init__(self):
        self.sentences = []
        self.vectorizer = None
        self.matrix = None

    # --------------------------------------------------------

    def build_from_text(self, text: str):
        """
        Build index directly from raw text.
        Used mainly for single-document workflows.
        """
        text = extract_core_text(text)
        self.sentences = split_sentences(text)

        if not self.sentences:
            self.matrix = None
            return

        self.vectorizer = TfidfVectorizer(
            analyzer="char_wb",
            ngram_range=(3, 5),
            min_df=1
        )

        self.matrix = self.vectorizer.fit_transform(self.sentences)

    # --------------------------------------------------------

    def _encode_query(self, query: str):
        """
        Encode query using existing vectorizer.
        """
        if not self.vectorizer:
            return None

        return self.vectorizer.transform([query])

    # --------------------------------------------------------

    def query(self, query: str, min_score: float = 0.15):
        """
        Query index and return evidence-backed result.

        Returns:
        [
          {
            "answer": str,
            "evidence": str,
            "score": float,
            "stitched_count": int
          }
        ]
        """

        if not query or not self.sentences or self.matrix is None:
            return []

        query_vec = self._encode_query(query)
        if query_vec is None:
            return []

        scores = cosine_similarity(query_vec, self.matrix)[0]

        best_idx = int(np.argmax(scores))
        best_score = float(scores[best_idx])

        if best_score < min_score:
            return []

        # ----------------------------------------------------
        # Sentence stitching (local, conservative)
        # ----------------------------------------------------

        indices = [best_idx]

        # include previous sentence if meaningful
        if best_idx - 1 >= 0 and scores[best_idx - 1] >= 0.5 * best_score:
            indices.insert(0, best_idx - 1)

        # include next sentence if meaningful
        if best_idx + 1 < len(scores) and scores[best_idx + 1] >= 0.5 * best_score:
            indices.append(best_idx + 1)

        stitched = " ".join(self.sentences[i] for i in indices)

        return [{
            "answer": stitched,
            "evidence": stitched,
            "score": best_score,
            "stitched_count": len(indices)
        }]
