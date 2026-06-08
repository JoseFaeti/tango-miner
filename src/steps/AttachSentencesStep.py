from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.steps.ProcessingStep import ProcessingStep
from src.SegmentedSentence import SegmentedSentence
from src.WordStats import Sentence, WordStats

from collections import defaultdict
from itertools import count
import heapq
import re
from bisect import bisect_right

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MAX_SENTENCES = 3
# Max number of example sentences stored per word

GLOBAL_AVERAGE_SCORE = 3000
# Fallback difficulty score when no known words exist in a sentence
# Range: ~100–1000 depending on your word scoring system
# Higher → treats unknown sentences as harder
# Lower → treats unknown sentences as easier (more likely to be selected)

IDEAL_LENGTH = 25
# Target sentence length in characters
# Range: 10–60 depending on corpus style
# Higher → prefers longer sentences
# Lower → prefers shorter, more compact sentences

LENGTH_DIVISOR = 15
# Controls how strongly length deviation is penalized
# Range: 5–50
# Lower → very strong preference for IDEAL_LENGTH (sharp penalty)
# Higher → more tolerant of length variation (weaker penalty)

UNKNOWN_WORD_PENALTY = 0.2
# Penalty weight for unknown words in sentence
# Range: 0.0–1.0
# Higher → strongly discourages sentences with unknown vocabulary
# Lower → allows more natural / noisy sentences

TOO_HARD_WORD_PENALTY = 0.4
# Penalty for words above target difficulty level
# Range: 0.0–1.0
# Higher → strongly avoids sentences that exceed user level
# Lower → allows more challenging sentences

SHORT_SENTENCE_PENALTY = 0.5
# Penalty multiplier for sentences with too few known words
# Range: 0.0–2.0
# Higher → strongly discourages short / low-information sentences
# Lower → allows short sentences to compete more often

MIN_WORD_COUNT = 6
# Minimum number of known words required before short-sentence penalty stops applying
# Range: 3–15
# Higher → forces richer sentences (bias toward longer sentences)
# Lower → allows shorter sentences to pass more easily

VARIANCE_DIVISOR = 100000
# Scales down penalty for variance in word difficulty within a sentence
# Range: 10,000–1,000,000
# Lower → variance matters more (mixed difficulty sentences penalized harder)
# Higher → variance mostly ignored


# ---------------------------------------------------------------------------
# Pipeline step
# ---------------------------------------------------------------------------

class AttachSentencesStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        attach_sentences(
            artifact.data,
            artifact.sentences,
            self.progress,
        )
        return artifact


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_japanese_char(c: str) -> bool:
    o = ord(c)
    return (
        0x3040 <= o <= 0x309F or
        0x30A0 <= o <= 0x30FF or
        0x4E00 <= o <= 0x9FFF
    )


def _percentile(values, p=0.9):
    s = sorted(values)
    if not s:
        return GLOBAL_AVERAGE_SCORE
    return s[min(int(len(s) * p), len(s) - 1)]


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def attach_sentences(word_data, segmented_sentences, progress_handler=None):
    total = len(segmented_sentences)

    candidates = defaultdict(dict)
    counter = count()

    word_get = word_data.get

    for i, seg in enumerate(segmented_sentences):
        if progress_handler and (i % 1000 == 0):
            progress_handler(ProcessingStep.SENTENCES, i, total) #, f"{i}/{total} sentences")

        lemma_surfaces = seg.lemma_surfaces

        # ------------------------------------------------------------
        # PHASE 1: sentence stats (single pass)
        # ------------------------------------------------------------
        scores = []
        unknown_count = 0

        for lemma in lemma_surfaces:
            ws = word_get(lemma)
            if ws is None:
                unknown_count += 1
            else:
                scores.append(ws.score)

        if not scores:
            continue

        mean = sum(scores) / len(scores)
        variance = sum((x - mean) ** 2 for x in scores) / len(scores)

        sentence_difficulty = _percentile(scores, 0.9)
        sorted_scores = sorted(scores)

        # ------------------------------------------------------------
        # PHASE 2: sentence-level penalties (once per sentence)
        # ------------------------------------------------------------
        text = seg.text
        text_len = len(text)

        penalty = _sentence_penalty_fast(
            text,
            scores,
            unknown_count,
            variance,
        )

        sentence_quality = _sentence_quality_penalty_fast(text)
        base_penalty = penalty + sentence_quality

        total_tokens = len(scores) + unknown_count
        if total_tokens == 0:
            continue

        # ------------------------------------------------------------
        # PHASE 3: per-lemma scoring (optimized)
        # ------------------------------------------------------------
        for lemma, surface in lemma_surfaces.items():
            ws = word_get(lemma)
            if ws is None:
                continue

            # fast O(log n) replacement for too_hard loop
            too_hard = len(sorted_scores) - bisect_right(sorted_scores, ws.score)

            lemma_adjustment = max(0.0, sentence_difficulty - ws.score)

            fitness = (
                base_penalty
                + lemma_adjustment
                + (too_hard / total_tokens) * TOO_HARD_WORD_PENALTY
            )

            sentence = Sentence(
                text=seg.text,
                tag=seg.tag,
                origin=seg.origin,
                surface_form=surface,
            )

            key = _sentence_dedupe_key_fast(text)
            bucket = candidates[lemma]

            existing = bucket.get(key)
            item = (fitness, next(counter), sentence)

            if existing is None or fitness < existing[0]:
                bucket[key] = item

    # ------------------------------------------------------------
    # FINALIZE
    # ------------------------------------------------------------
    for lemma, bucket in candidates.items():
        ws = word_get(lemma)
        if not ws:
            continue

        best = heapq.nsmallest(MAX_SENTENCES, bucket.values(), key=lambda x: x[0])
        ws.sentences = [b[2] for b in best]

    progress_handler(ProcessingStep.SENTENCES, 1, 1)


# ---------------------------------------------------------------------------
# Penalties
# ---------------------------------------------------------------------------

def _sentence_penalty_fast(text, scores, unknown_count, variance):
    penalty = 0.0

    total_tokens = len(scores) + unknown_count
    if total_tokens == 0:
        return 0.0

    penalty += variance / VARIANCE_DIVISOR
    penalty += (unknown_count / total_tokens) * UNKNOWN_WORD_PENALTY
    penalty += abs(len(text) - IDEAL_LENGTH) / LENGTH_DIVISOR

    if len(scores) < MIN_WORD_COUNT:
        penalty += (MIN_WORD_COUNT - len(scores)) * SHORT_SENTENCE_PENALTY

    return penalty


def _sentence_quality_penalty_fast(text):
    penalty = 0.0

    penalty += text.count("[...]") * 0.25
    penalty += text.count("「") * 0.05

    if text.count("。") + text.count("！") + text.count("？") > 1:
        penalty += 0.5

    visible = 0
    jp = 0

    for c in text:
        if c.isspace():
            continue
        visible += 1
        if _is_japanese_char(c):
            jp += 1

    if visible:
        penalty += 1 - (jp / visible)

    if not text.endswith(("。", "！", "？")):
        penalty += 0.5

    return penalty


# ---------------------------------------------------------------------------
# Dedup key
# ---------------------------------------------------------------------------

def _sentence_dedupe_key_fast(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip()

    if ">" not in normalized:
        return normalized

    prefix, body = normalized.split(">", 1)

    if body and any("\u4e00" <= c <= "\u9fff" for c in body):
        return body.strip()

    return normalized