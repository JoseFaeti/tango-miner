from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep
from .SegmentedSentence import SegmentedSentence
from .WordStats import Sentence, WordStats

from collections import defaultdict
from heapq import heappush, heappushpop
from itertools import count

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MAX_SENTENCES = 3

GLOBAL_AVERAGE_SCORE = 3000
SMOOTHING_WEIGHT = 5

IDEAL_LENGTH = 50
LENGTH_DIVISOR = 100

UNKNOWN_WORD_PENALTY = 0.2
TOO_HARD_WORD_PENALTY = 0.5
SHORT_SENTENCE_PENALTY = 0.3

MIN_WORD_COUNT = 4
VARIANCE_DIVISOR = 100000


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

    candidates = defaultdict(list)
    counter = count()

    for i, seg in enumerate(segmented_sentences):
        if progress_handler:
            progress_handler(ProcessingStep.SENTENCES, i, total)

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

            over_penalty = _over_level_penalty(scores, ws.score)
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

            heap = candidates[lemma]

            item = (-fitness, next(counter), sentence)

            if len(heap) < MAX_SENTENCES:
                heappush(heap, item)
            else:
                heappushpop(heap, item)

    if progress_handler:
        progress_handler(ProcessingStep.SENTENCES, total, total)

    # finalize
    for lemma, heap in candidates.items():
        ws = word_data.get(lemma)
        if not ws:
            continue

        ws.sentences = [
            item[2]
            for item in sorted(heap, reverse=True)
        ]


def _over_level_penalty(scores, target):
    penalty = 0.0
    for s in scores:
        if s > target:
            diff = s - target
            penalty += diff * diff  # quadratic penalty
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
