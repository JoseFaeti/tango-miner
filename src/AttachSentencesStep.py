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

IDEAL_LENGTH = 25
LENGTH_DIVISOR = 100

UNKNOWN_WORD_PENALTY = 0.2
HARDER_WORD_PENALTY = 0.1
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
# Main function
# ---------------------------------------------------------------------------

def attach_sentences(
    word_data: dict,
    segmented_sentences: list[SegmentedSentence],
    progress_handler=None,
):
    """
    Build a ranked candidate pool per lemma using a heap.
    No greedy attachment is performed anymore.
    """

    total = len(segmented_sentences)

    # lemma -> min-heap of (-fitness, Sentence, tag)
    candidates = defaultdict(list)
    counter = count()

    for i, seg in enumerate(segmented_sentences):
        if progress_handler:
            progress_handler(ProcessingStep.SENTENCES, i, total)

        scores, unknown_count, mean_score, variance = _compute_sentence_stats(
            seg,
            word_data,
        )

        sentence_difficulty = _sentence_difficulty_from_stats(
            scores,
        )

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

            fitness = abs(ws.score - sentence_difficulty) + penalty

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

    # finalize: assign best sentences per word
    for lemma, heap in candidates.items():
        ws = word_data.get(lemma)
        if not ws:
            continue

        ws.sentences = [
            item[2]
            for item in sorted(heap, reverse=True)
        ]


# ---------------------------------------------------------------------------
# Sentence statistics (computed once per sentence)
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


def _sentence_difficulty_from_stats(scores):
    """
    Smoothed sentence difficulty (prevents short sentence bias)
    """
    if not scores:
        return GLOBAL_AVERAGE_SCORE

    return (
        sum(scores) + GLOBAL_AVERAGE_SCORE * SMOOTHING_WEIGHT
    ) / (
        len(scores) + SMOOTHING_WEIGHT
    )


# ---------------------------------------------------------------------------
# Penalty model
# ---------------------------------------------------------------------------

def _sentence_penalty(
    ws: WordStats,
    seg: SegmentedSentence,
    scores,
    unknown_count,
    variance,
):
    penalty = 0.0

    # variance penalty (diversity of word difficulty)
    penalty += variance / VARIANCE_DIVISOR

    # unknown words penalty
    penalty += unknown_count * UNKNOWN_WORD_PENALTY

    # sentence length penalty (prefer medium length)
    penalty += abs(len(seg.text) - IDEAL_LENGTH) / LENGTH_DIVISOR

    # too few known words penalty
    if len(scores) < MIN_WORD_COUNT:
        penalty += (MIN_WORD_COUNT - len(scores)) * SHORT_SENTENCE_PENALTY

    # words significantly harder than target
    penalty += sum(
        1
        for s in scores
        if s > ws.score * 1.2
    ) * HARDER_WORD_PENALTY

    return penalty