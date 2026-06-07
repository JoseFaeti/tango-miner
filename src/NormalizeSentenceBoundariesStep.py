from __future__ import annotations

import re
import statistics
from pathlib import Path
from typing import List

from .Artifact import Artifact
from .PipelineStep import PipelineStep


# -------------------------------------------------------------------
# Precompiled regex (module scope = zero re-init cost per step)
# -------------------------------------------------------------------

RE_HARD_WRAP_JOIN = re.compile(r"(?<![。.!?])\n(?!\n)")
RE_MULTI_NEWLINE = re.compile(r"\n{2,}")
RE_WHITESPACE = re.compile(r"[ \t]+")

RE_SENT_SPLIT = re.compile(r"(?<=[。.!?])\s+")

JP_CONTINUATIONS = (
    "そして", "しかし", "また", "それ", "これ", "だから", "そのため"
)


# -------------------------------------------------------------------
# Pipeline step
# -------------------------------------------------------------------

class NormalizeSentenceBoundariesStep(PipelineStep):
    def __init__(self, min_lines: int = 5):
        self.min_lines = min_lines

    def process(self, artifact: Artifact) -> Artifact:
        files: list[tuple[Path, str]] = artifact.data
        results: list[tuple[Path, list[str]]] = []

        for path, text in files:
            sentences, _ = normalize_sentence_boundaries(text, self.min_lines)
            results.append((path, sentences))

        return Artifact(results)


def normalize_sentence_boundaries(text: str, min_lines: int = 5):
    lines = text.split("\n")

    if is_dialogue_line_mode(lines):
        mode = "line_dialogue"
    elif is_sentence_list(lines):
        mode = "sentence_list"
    else:
        mode = "paragraph"

    if mode == "line_dialogue":
        sentences = sentence_per_line(lines)

    elif mode == "sentence_list":
        sentences = sentence_per_line(lines)

    elif mode == "paragraph":
        sentences = paragraph_based(text)

    else:
        sentences = fallback(text)

    return sentences, mode

# -------------------------------------------------------------------
# Mode detection
# -------------------------------------------------------------------

def detect_mode(lines: List[str], min_lines: int = 5) -> str:
    clean = [l for l in lines if l]
    n = len(clean)

    if n < min_lines:
        return "paragraph_based"

    punct = 0
    lengths = []

    for l in clean:
        if l.endswith(("。", ".", "!", "?")):
            punct += 1
        lengths.append(len(l))

    punct_ratio = punct / n

    mean = sum(lengths) / n
    var = sum((x - mean) ** 2 for x in lengths) / n
    cv = (var ** 0.5) / (mean + 1e-6)

    cont = 0
    for l in clean[1:]:
        if l.startswith(JP_CONTINUATIONS):
            cont += 1

    cont_ratio = cont / n

    if punct_ratio > 0.7:
        return "sentence_per_line"

    if cont_ratio > 0.3 and cv < 0.6:
        return "hard_wrap"

    if punct_ratio < 0.3 and cv < 0.45:
        return "hard_wrap"

    return "paragraph_based"


# -------------------------------------------------------------------
# Processing modes
# -------------------------------------------------------------------

def is_line_sentence_mode(lines: list[str]) -> bool:
    clean = [l.strip() for l in lines if l.strip()]
    if len(clean) < 3:
        return False

    # strong signal: short lines + high independence
    avg_len = sum(len(l) for l in clean) / len(clean)

    if avg_len > 40:
        return False  # likely prose, not dialogue

    # most lines are self-contained (no trailing connectors)
    self_contained = 0

    for l in clean:
        if l.endswith(("、", "…", "〜")):
            continue
        if len(l) <= 30:
            self_contained += 1

    ratio = self_contained / len(clean)

    return ratio > 0.7


def is_dialogue_line_mode(lines: list[str]) -> bool:
    clean = [l.strip() for l in lines if l.strip()]
    if len(clean) < 5:
        return False

    # short lines dominate
    avg_len = sum(len(l) for l in clean) / len(clean)
    if avg_len > 45:
        return False

    # high line-boundary entropy (many independent fragments)
    short_lines = sum(1 for l in clean if len(l) <= 40)
    ratio = short_lines / len(clean)

    if ratio < 0.7:
        return False

    # weak punctuation dependence
    punctuation_ratio = sum(l.endswith(("。", "！", "？")) for l in clean) / len(clean)

    # dialogue scripts often have LOW punctuation ratio
    return punctuation_ratio < 0.4 and ratio > 0.7


def is_sentence_list(lines: list[str]) -> bool:
    clean = [l.strip() for l in lines if l.strip()]
    if len(clean) < 3:
        return False

    # strong signal: many short independent lines
    avg_len = sum(len(l) for l in clean) / len(clean)
    if avg_len > 50:
        return False

    short_ratio = sum(len(l) <= 40 for l in clean) / len(clean)

    # sentence lists tend to be mostly self-contained lines
    return short_ratio > 0.8


def sentence_per_line(lines: List[str]) -> List[str]:
    # minimal allocation, no strip
    return [l.strip() for l in lines if l.strip()]


def merge_hard_wrap(text: str) -> List[str]:
    text = RE_HARD_WRAP_JOIN.sub(" ", text)
    text = RE_MULTI_NEWLINE.sub("\n\n", text)
    text = RE_WHITESPACE.sub(" ", text)

    sentences = text.replace("\n\n", "\n").split("\n")
    return [s for s in sentences if s]


def paragraph_based(text: str) -> List[str]:
    text = RE_MULTI_NEWLINE.sub("\n\n", text)
    text = RE_WHITESPACE.sub(" ", text)

    paragraphs = text.split("\n\n")

    out = []
    extend = out.extend

    for p in paragraphs:
        if p:
            extend(split_sentences_fast(p))

    return out


def fallback(text: str) -> List[str]:
    return split_sentences_fast(text)


# -------------------------------------------------------------------
# Sentence splitting
# -------------------------------------------------------------------

def split_sentences_fast(text: str) -> List[str]:
    parts = RE_SENT_SPLIT.split(text)
    return [p for p in parts if p]