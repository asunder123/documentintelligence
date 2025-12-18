# ingestion/extractor.py
# Document-type aware text extraction
# Responsibility: convert files → clean text (ONLY)

import json
import re
import zipfile
from typing import Any

# Optional libraries (graceful degradation)
try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    import openpyxl
except Exception:
    openpyxl = None


# ============================================================
# Utilities
# ============================================================

def _safe_decode(data: bytes) -> str:
    try:
        return data.decode("utf-8")
    except Exception:
        return data.decode("latin1", errors="ignore")


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


# ============================================================
# Plain text formats
# ============================================================

def _extract_text_plain(uploaded_file) -> str:
    return _safe_decode(uploaded_file.read())


# ============================================================
# JSON
# ============================================================

def _extract_text_json(uploaded_file) -> str:
    raw = _safe_decode(uploaded_file.read())
    try:
        obj = json.loads(raw)
        return json.dumps(obj, indent=2)
    except Exception:
        return raw


# ============================================================
# HTML
# ============================================================

def _extract_text_html(uploaded_file) -> str:
    text = _safe_decode(uploaded_file.read())

    # Remove scripts and styles
    text = re.sub(r"<script.*?>.*?</script>", " ", text, flags=re.S)
    text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.S)

    # Strip tags
    text = re.sub(r"<[^>]+>", " ", text)

    return _normalize_whitespace(text)


# ============================================================
# DOCX
# ============================================================

def _extract_text_docx(uploaded_file) -> str:
    try:
        with zipfile.ZipFile(uploaded_file) as z:
            xml = z.read("word/document.xml").decode("utf-8", errors="ignore")
            text = re.sub(r"<[^>]+>", " ", xml)
            return _normalize_whitespace(text)
    except Exception:
        return ""


# ============================================================
# PDF
# ============================================================

def _extract_text_pdf(uploaded_file) -> str:
    if pdfplumber is not None:
        try:
            with pdfplumber.open(uploaded_file) as pdf:
                pages = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        pages.append(t)
                return _normalize_whitespace("\n".join(pages))
        except Exception:
            pass

    # Fallback: raw decode (lossy but safe)
    return _safe_decode(uploaded_file.read())


# ============================================================
# XLSX (Excel)
# ============================================================

def _extract_text_xlsx(uploaded_file) -> str:
    if openpyxl is None:
        return ""

    try:
        wb = openpyxl.load_workbook(uploaded_file, data_only=True)
        rows = []

        for sheet in wb.worksheets:
            rows.append(f"Sheet: {sheet.title}")
            for row in sheet.iter_rows(values_only=True):
                line = " | ".join(str(cell) for cell in row if cell is not None)
                if line:
                    rows.append(line)

        return _normalize_whitespace("\n".join(rows))
    except Exception:
        return ""


# ============================================================
# Public API
# ============================================================

def extract_text_from_file(uploaded_file: Any) -> str:
    """
    Master extractor.
    Routes to document-type specific logic.
    Always returns a string.
    """

    if uploaded_file is None:
        return ""

    name = uploaded_file.name.lower()

    if name.endswith((".txt", ".md", ".csv")):
        return _extract_text_plain(uploaded_file)

    if name.endswith(".json"):
        return _extract_text_json(uploaded_file)

    if name.endswith((".html", ".htm")):
        return _extract_text_html(uploaded_file)

    if name.endswith(".docx"):
        return _extract_text_docx(uploaded_file)

    if name.endswith(".pdf"):
        return _extract_text_pdf(uploaded_file)

    if name.endswith(".xlsx"):
        return _extract_text_xlsx(uploaded_file)

    # Fallback for unknown formats
    return _safe_decode(uploaded_file.read())
