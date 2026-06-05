from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep
from .SegmentedSentence import SegmentedSentence
from .WordStats import Sentence, WordStats

from collections import defaultdict
from itertools import count
import re

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
# Main
# ---------------------------------------------------------------------------

def attach_sentences(
    word_data: dict,
    segmented_sentences: list[SegmentedSentence],
    progress_handler=None,
):
    total = len(segmented_sentences)

    candidates = defaultdict(dict)
    counter = count()

    for i, seg in enumerate(segmented_sentences):
        if progress_handler and (i % 1000 == 0):
            progress_handler(ProcessingStep.SENTENCES, i, total, f"{i}/{total} sentences")

        scores, unknown_count, mean_score, variance = _compute_sentence_stats(
            seg,
            word_data,
        )

        if not scores:
            sentence_difficulty = GLOBAL_AVERAGE_SCORE
        else:
            # percentile-based difficulty (prevents hard-word masking)
            sentence_difficulty = _percentile(scores, 0.9)

        for lemma, surface in seg.lemma_surfaces.items():
            ws = word_data.get(lemma)
            if ws is None:
                continue

            penalty = _sentence_penalty(
                ws,
                seg,
                scores,
                unknown_count,
                variance,
            )
            penalty += sentence_quality_penalty(seg.text)

            penalty += (_over_level_penalty(scores, ws.score) / len(scores)) * 1.5

            # ----------------------------------------------------------------
            # ASYMMETRIC FITNESS
            # ----------------------------------------------------------------
            over_difficulty = max(
                0.0,
                sentence_difficulty - ws.score,
            )

            fitness = over_difficulty + penalty

            sentence = Sentence(
                text=seg.text,
                tag=seg.tag,
                origin=seg.origin,
                surface_form=surface,
            )

            candidates_by_text = candidates[lemma]
            item = (fitness, next(counter), sentence)
            sentence_key = _sentence_dedupe_key(sentence.text)
            existing = candidates_by_text.get(sentence_key)

            if existing is None or fitness < existing[0]:
                candidates_by_text[sentence_key] = item

    if progress_handler:
        progress_handler(ProcessingStep.SENTENCES, total, total)

    # finalize
    for lemma, candidates_by_text in candidates.items():
        ws = word_data.get(lemma)
        if not ws:
            continue

        ws.sentences = [
            item[2]
            for item in sorted(candidates_by_text.values())[:MAX_SENTENCES]
        ]


def _over_level_penalty(scores, target):
    penalty = 0.0
    for s in scores:
        if s > target:
            diff = s - target
            penalty += diff * diff  # quadratic penalty
    return penalty


def _sentence_dedupe_key(text: str) -> str:
    """
    Return a stable key for deciding whether two attached sentences are the
    same learning sentence. Some script dumps prefix the visible Japanese text
    with cue metadata such as "F4 AD 01 ...>", which should not make a
    duplicate sentence look unique.
    """
    normalized = re.sub(r"\s+", " ", text).strip()

    if ">" not in normalized:
        return normalized

    prefix, body = normalized.split(">", 1)

    if body and _contains_japanese(body) and not _contains_japanese(prefix):
        return body.strip()

    return normalized


def _contains_japanese(text: str) -> bool:
    return any(
        "\u3040" <= c <= "\u309F"
        or "\u30A0" <= c <= "\u30FF"
        or "\u4E00" <= c <= "\u9FFF"
        for c in text
    )


def sentence_quality_penalty(text: str) -> float:
    penalty = 0.0

    penalty += text.count("[...]") * 0.25
    penalty += text.count("「") * 0.05

    if text.count("。") + text.count("！") + text.count("？") > 1:
        penalty += 0.5

    visible_chars = sum(1 for c in text if not c.isspace())
    if visible_chars:
        japanese_chars = sum(1 for c in text if _contains_japanese(c))
        non_japanese_ratio = 1 - (japanese_chars / visible_chars)
        penalty += non_japanese_ratio

    if not text.endswith(("。", "！", "？")):
        penalty += 0.15

    return penalty


# ---------------------------------------------------------------------------
# Sentence stats
# ---------------------------------------------------------------------------

def _compute_sentence_stats(seg, word_data):
    scores = []
    unknown = 0

    for lemma in seg.lemma_surfaces:
        ws = word_data.get(lemma)
        if ws is None:
            unknown += 1
        else:
            scores.append(ws.score)

    if scores:
        mean = sum(scores) / len(scores)
        variance = (
            sum((x - mean) ** 2 for x in scores)
            / len(scores)
        )
    else:
        mean = GLOBAL_AVERAGE_SCORE
        variance = 0

    return scores, unknown, mean, variance


def _percentile(values, p=0.9):
    s = sorted(values)
    if not s:
        return GLOBAL_AVERAGE_SCORE
    idx = int(len(s) * p)
    idx = min(idx, len(s) - 1)
    return s[idx]


# ---------------------------------------------------------------------------
# Penalty model (non-core now, mostly stability)
# ---------------------------------------------------------------------------

def _sentence_penalty(
    ws: WordStats,
    seg: SegmentedSentence,
    scores,
    unknown_count,
    variance,
):
    penalty = 0.0
    total_tokens = len(scores) + unknown_count

    # variance (reduces mixed-level sentences)
    penalty += variance / VARIANCE_DIVISOR

    # unknown words
    penalty += (unknown_count / total_tokens) * UNKNOWN_WORD_PENALTY

    # sentence length
    penalty += abs(len(seg.text) - IDEAL_LENGTH) / LENGTH_DIVISOR

    # too few known words
    if len(scores) < MIN_WORD_COUNT:
        penalty += (MIN_WORD_COUNT - len(scores)) * SHORT_SENTENCE_PENALTY

    # STRICT: words above target level are expensive
    too_hard = sum(
        1 for s in scores
        if s > ws.score
    )

    penalty += (too_hard / total_tokens) * TOO_HARD_WORD_PENALTY

    return penalty
