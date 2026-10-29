from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.RunConfig import SentencesConfig
from src.steps.ProcessingStep import ProcessingStep
from src.SegmentedSentence import SegmentedSentence
from src.WordStats import Sentence, WordStats

from collections import defaultdict
from dataclasses import replace
from itertools import count
import heapq
import re
from bisect import bisect_right

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Backward-compatible defaults for direct callers of this module.
MAX_SENTENCES = SentencesConfig().max_sentences
GLOBAL_AVERAGE_SCORE = 3000  # Used only by the standalone percentile helper.


# ---------------------------------------------------------------------------
# Pipeline step
# ---------------------------------------------------------------------------

class AttachSentencesStep(PipelineStep):
    def __init__(self, config: SentencesConfig | None = None):
        self.config = config if config is not None else SentencesConfig()
        self._processing_step = ProcessingStep.SENTENCES


    def process(self, artifact: Artifact) -> Artifact:
        attach_sentences(
            artifact.data,
            artifact.sentences,
            self.progress,
            config=self.config,
        )

        self.done(f"{sum(len(sentences) for word in artifact.data for sentences in word)} sentences added.")

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

def attach_sentences(word_data, segmented_sentences, progress_handler=None, config: SentencesConfig | None = None):
    config = config if config is not None else SentencesConfig()
    total = len(segmented_sentences)

    if not word_data or not segmented_sentences:
        return

    max_score = max(ws.score for ws in word_data.values())

    candidates = defaultdict(dict)
    grammar_examples = defaultdict(list)
    for stats in word_data.values():
        if stats.pos == ('grammar',):
            for index, example in enumerate(stats.sentences):
                grammar_examples[(example.origin, example.text)].append((stats, index))
    counter = count()

    word_get = word_data.get

    for i, seg in enumerate(segmented_sentences):
        if progress_handler and (i % 1000 == 0):
            progress_handler(i, total) #, f"{i}/{total} sentences")

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

        # Don't skip sentences with no scored tokens — they still contain
        # the target word and may be the only available sentence for it.
        # Use worst-case defaults so they rank below quality sentences.
        if scores:
            mean = sum(scores) / len(scores)
            variance = sum((x - mean) ** 2 for x in scores) / len(scores)
            sentence_difficulty = _percentile(scores, 0.9)
            sorted_scores = sorted(scores)
        else:
            mean = 0.0
            variance = 0.0
            sentence_difficulty = 0.0
            sorted_scores = []

        # Grammar examples were selected from raw tokens before vocabulary
        # scoring. Give them the same sentence mean used by lexical examples.
        key = (str(seg.origin or ''), seg.text)
        for stats, index in grammar_examples.pop(key, ()):
            stats.sentences[index] = replace(stats.sentences[index], score=round(mean, 2))

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
            config=config,
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
            if ws.definition_components is not None and seg.lemma_components.get(lemma) not in ws.definition_components:
                continue

            # Fraction of sentence words that score harder than this word [0, 1]
            too_hard = len(sorted_scores) - bisect_right(sorted_scores, ws.score)

            # Symmetric distance: penalize sentences both harder AND easier than the word
            lemma_adjustment = abs(mean - ws.score) / max_score

            fitness = (
                base_penalty
                + lemma_adjustment * config.difficulty_weight
                + (too_hard / total_tokens) * config.too_hard_word_penalty
            )

            sentence = Sentence(
                text = seg.text,
                tag = seg.tag,
                origin = seg.origin,
                surface_form = surface,
                score = round(mean, 2),
                span = seg.lemma_spans.get(lemma)
            )

            key = _sentence_dedupe_key_fast(text)
            bucket = candidates[lemma]

            existing = bucket.get(key)
            item = (fitness, next(counter), sentence)

            if existing is None or fitness < existing[0]:
                bucket[key] = item

    # ------------------------------------------------------------
    # FINALIZE — primary pass
    # ------------------------------------------------------------
    for lemma, bucket in candidates.items():
        ws = word_get(lemma)
        if not ws:
            continue

        best = heapq.nsmallest(config.max_sentences, bucket.values(), key=lambda x: x[0])
        ws.sentences = [b[2] for b in best]

    # ------------------------------------------------------------
    # FALLBACK — try to fill the configured number of sentences per word
    #
    # For words that came out of the primary pass with fewer
    # sentences than the configured limit (including zero), do a second
    # sweep over all segmented sentences and attach whatever is
    # available, ignoring all quality filters.  Worst-case the
    # learner gets a mediocre sentence; best-case they get
    # something useful they would otherwise have missed.
    # ------------------------------------------------------------
    words_needing_fallback = {
        lemma
        for lemma, ws in word_data.items()
        if len(ws.sentences) < config.max_sentences
    }

    if words_needing_fallback:
        # Build an index: lemma → list[Sentence] not yet attached
        fallback_pool = defaultdict(list)

        for seg in segmented_sentences:
            for lemma, surface in seg.lemma_surfaces.items():
                if lemma not in words_needing_fallback:
                    continue

                ws = word_get(lemma)
                if ws is None:
                    continue
                if ws.definition_components is not None and seg.lemma_components.get(lemma) not in ws.definition_components:
                    continue

                sentence = Sentence(
                    text = seg.text,
                    tag = seg.tag,
                    origin = seg.origin,
                    surface_form = surface,
                    score = ws.score * -1,
                    span = seg.lemma_spans.get(lemma)
                )

                # skip exact duplicates already attached
                already = {s.text for s in ws.sentences}
                if sentence.text not in already:
                    fallback_pool[lemma].append(sentence)

        for lemma, pool in fallback_pool.items():
            ws = word_get(lemma)
            if ws is None:
                continue

            needed = config.max_sentences - len(ws.sentences)
            if needed <= 0:
                continue

            already = {s.text for s in ws.sentences}
            for sentence in pool:
                if needed <= 0:
                    break
                if sentence.text not in already:
                    ws.sentences.append(sentence)
                    already.add(sentence.text)
                    needed -= 1


# ---------------------------------------------------------------------------
# Penalties
# ---------------------------------------------------------------------------

def _sentence_penalty_fast(text, scores, unknown_count, variance, config: SentencesConfig | None = None):
    config = config if config is not None else SentencesConfig()
    penalty = 0.0

    total_tokens = len(scores) + unknown_count
    if total_tokens == 0:
        return 0.0

    penalty += variance / config.variance_divisor
    penalty += (unknown_count / total_tokens) * config.unknown_word_penalty
    penalty += abs(len(text) - config.ideal_length) / config.length_divisor

    if len(scores) < config.min_word_count:
        penalty += (config.min_word_count - len(scores)) * config.short_sentence_penalty

    return penalty


def _sentence_quality_penalty_fast(text):
    penalty = 0.0

    penalty += text.count("[...]") * 0.25
    penalty += text.count("「") * 0.05

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
    if ">" not in text:
        return text

    prefix, body = text.split(">", 1)

    if body and any("\u4e00" <= c <= "\u9fff" for c in body):
        return body.strip()

    return text
