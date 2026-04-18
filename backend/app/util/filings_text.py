from __future__ import annotations

import re

from bs4 import BeautifulSoup

_NEGATING_PREFIXES = (
    "no substantial doubt",
    "not substantial doubt",
    "no material uncertainty",
    "without substantial doubt",
)
_GOING_CONCERN_PHRASES = (
    "substantial doubt about the company's ability to continue as a going concern",
    "substantial doubt about its ability to continue as a going concern",
    "substantial doubt about our ability to continue as a going concern",
    "raises substantial doubt",
    "raise substantial doubt",
)
_CH11_PATTERN = re.compile(r"\bchapter\s+11\b", re.IGNORECASE)
_WS_PATTERN = re.compile(r"\s+")


def strip_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(" ")
    return _WS_PATTERN.sub(" ", text).strip()


def _context(text: str, idx: int, window: int = 80) -> str:
    lo = max(0, idx - window)
    hi = min(len(text), idx + window)
    return text[lo:hi].lower()


def detect_going_concern(text_lower: str) -> bool:
    """True iff affirmative going-concern language appears and isn't negated by a nearby prefix."""
    for phrase in _GOING_CONCERN_PHRASES:
        start = 0
        while True:
            idx = text_lower.find(phrase, start)
            if idx < 0:
                break
            ctx = _context(text_lower, idx, window=120)
            if not any(neg in ctx for neg in _NEGATING_PREFIXES):
                return True
            start = idx + len(phrase)
    return False


def count_chapter_11(text: str) -> int:
    return len(_CH11_PATTERN.findall(text))


def analyze_filing_html(html: str) -> tuple[bool, int]:
    """Return (going_concern_flag, chapter_11_mention_count) for a 10-K/Q HTML blob."""
    text = strip_html(html)
    return detect_going_concern(text.lower()), count_chapter_11(text)
