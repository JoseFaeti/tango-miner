from __future__ import annotations

import re
import statistics
from pathlib import Path
from typing import List

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep


# -------------------------------------------------------------------
# Precompiled regex (module scope = zero re-init cost per step)
# -------------------------------------------------------------------

RE_HARD_WRAP_JOIN = re.compile(r"(?<![。.!?])\n(?!\n)")
RE_MULTI_NEWLINE = re.compile(r"\n{2,}")
RE_WHITESPACE = re.compile(r"[ \t]+")

RE_SENT_SPLIT = re.compile(r"(?<=[。.!?])\s+")
PUNCT_CHARS = r'[。．！？!?]'
RE_SENTENCE_BOUNDARY = re.compile(rf'({PUNCT_CHARS}+)\s*(?=[^\s。．！？!?])')

JP_CONTINUATIONS = (
    "そして", "しかし", "また", "それ", "これ", "だから", "そのため"
)


# -------------------------------------------------------------------
# Pipeline step
# -------------------------------------------------------------------

class ExtractSentencesStep(PipelineStep):
    def __init__(self, min_lines: int = 5):
        self.min_lines = min_lines
        self._processing_step = ProcessingStep.SENTENCE_EXTRACTION


    def process(self, artifact: Artifact) -> Artifact:
        files: list[tuple[Path, str]] = artifact.data
        results: list[tuple[Path, list[str]]] = []

        for path, text in files:
            self.progress(len(results), len(files))

            sentences, _ = normalize_sentence_boundaries(
                text,
                self.min_lines)

            results.append((path, sentences))

        self.done(f"{sum(len(sentences) for _, sentences in results)} sentences found.")

        return Artifact(results)


def normalize_sentence_boundaries(text: str, min_lines: int = 5):
    lines = text.split("\n")

    if is_indented_continuation_mode(lines):
        mode = "indented_continuation"
    elif is_dialogue_line_mode(lines):
        mode = "line_dialogue"
    elif is_sentence_list(lines):
        mode = "sentence_list"
    else:
        mode = "paragraph"

    if mode == "indented_continuation":
        sentences = join_indented_continuations(lines)
    elif mode == "line_dialogue":
        sentences = sentence_per_line(lines)
    elif mode == "sentence_list":
        sentences = sentence_per_line(lines)
    elif mode == "paragraph":
        sentences = paragraph_based(text)
    else:
        sentences = fallback(text)

    sentences = [
        split
        for sentence in sentences
        for split in split_on_punctuation(sentence)
    ]

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
    text = RE_HARD_WRAP_JOIN.sub("　", text)
    text = RE_MULTI_NEWLINE.sub("\n\n", text)
    text = RE_WHITESPACE.sub("　", text)

    sentences = text.replace("\n\n", "\n").split("\n")
    return [s for s in sentences if s]


def paragraph_based(text: str) -> List[str]:
    text = RE_MULTI_NEWLINE.sub("\n\n", text)
    text = RE_WHITESPACE.sub("　", text)

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


def split_on_punctuation(text: str) -> list[str]:
    """
    Split a string into sentences on punctuation boundaries, keeping the
    punctuation attached to the sentence it terminates.
    Handles runs like ???, !!!, ?!! etc.
    """
    result = []
    prev = 0

    for m in RE_SENTENCE_BOUNDARY.finditer(text):
        punct_end = m.start() + len(m.group(1))
        chunk = text[prev:punct_end].strip()
        if chunk:
            result.append(chunk)
        prev = m.end()

    tail = text[prev:].strip()
    if tail:
        result.append(tail)

    return result


# -------------------------------------------------------------------
# Indentation-based continuation detection
# -------------------------------------------------------------------

def measure_indent(line: str) -> int:
    """
    Count leading whitespace in visual columns.
    Full-width space (U+3000) counts as 2, tab as 4, regular space as 1.
    """
    count = 0
    for ch in line:
        if ch == ' ':
            count += 1
        elif ch == '\t':
            count += 4
        elif ch == '\u3000':
            count += 2
        else:
            break
    return count


def is_indented_continuation_mode(lines: list[str]) -> bool:
    """
    Detect files where indentation encodes sentence grouping:
    a line indented MORE than its predecessor continues the previous sentence;
    a line indented the same or less starts a new one.

    Signal: at least 25% of consecutive non-empty line pairs show a strict
    indent increase, AND at least two distinct indent levels are present.
    """
    non_empty = [l for l in lines if l.strip()]
    if len(non_empty) < 4:
        return False

    indents = [measure_indent(l) for l in non_empty]

    if len(set(indents)) < 2:
        return False

    continuations = sum(1 for a, b in zip(indents, indents[1:]) if b > a)
    return continuations / (len(indents) - 1) >= 0.25


def join_indented_continuations(lines: list[str]) -> list[str]:
    non_empty = [l for l in lines if l.strip()]
    if not non_empty:
        return []

    result = []
    current_parts = [non_empty[0].strip()]
    prev_indent = measure_indent(non_empty[0])

    for line in non_empty[1:]:
        indent = measure_indent(line)
        if indent < prev_indent:
            result.append("".join(current_parts))
            current_parts = [line.strip()]
        else:
            current_parts.append(line.strip())
        prev_indent = indent

    if current_parts:
        result.append("".join(current_parts))

    return result