from collections import OrderedDict
from pathlib import Path
import csv
import math
import re

from src.Artifact import Artifact
from src.Column import Column
from src.PipelineStep import PipelineStep
from src.steps.ProcessingStep import ProcessingStep


class FilterFrequencyStep(PipelineStep):
    def __init__(self, min_frequency: int, keep_percent: int = 98, min_frequency_per_source: float = 1.1):
        self._processing_step = ProcessingStep.FILTERING
        self.min_frequency = min_frequency
        self.keep_percent = keep_percent
        self.min_frequency_per_source = min_frequency_per_source

    def process(self, artifact: Artifact) -> Artifact:
        data = filter_useful_words(artifact.data, min_frequency=self.min_frequency, keep_percent=self.keep_percent, progress_handler=self.progress, min_frequency_per_source=self.min_frequency_per_source)
        return Artifact(data, sentences=artifact.sentences)


# Pure vocal imitations only; ordinary lexical onomatopoeia and words such as
# 母, 笑う, and はは (mother) remain eligible when evidence is ambiguous.
_VOCALIZATION = re.compile(r'^(?:[あアわワ]?[はハ]{2,}|[ぐグうウ]?[ふフ]{2,}|[えエ]?[へヘ]{2,}|[ひヒ]{3,}|[はハ][ぁァあアー][はハ][ぁァあアー])$')


def _is_pure_vocalization(word, stats):
    if not _VOCALIZATION.fullmatch(word) or not stats.component_counts:
        return False
    analyses = [parts for parts, count in stats.component_counts.items() if count > 0]
    # A candidate aggregates multiple analyzer readings. One malformed split
    # should not keep a clearly evidenced sound, but a genuine lexical noun
    # analysis of the same form remains an ambiguity and must be retained.
    if any(len(parts) == 1 and parts[0].pos and parts[0].pos[0] == '名詞'
           and parts[0].surface == word for parts in analyses):
        return False
    return any(
        parts and ''.join(part.surface for part in parts) == word
        and any(part.pos and part.pos[0] == '感動詞' for part in parts)
        and all(part.pos and (part.pos[0] == '感動詞'
                or len(parts) > 1 and part.surface in {'ぐ', 'グ'} and part.pos[0] == '名詞'
                or len(parts) > 1 and part.surface in {'は', 'ハ'} and part.pos[0] == '助詞')
                for part in parts)
        for parts in analyses
    )


def _has_supported_base_expression(stats, candidates, threshold):
    """Retain an inflected expression for review when its base also occurs."""
    if not stats.dictionary_expression_occurrences:
        return False
    for components in stats.component_counts:
        if (len(components) < 3 or components[-1].surface not in ('て', 'で')
                or not components[-1].pos or components[-1].pos[0] != '助詞'
                or not components[-2].pos or components[-2].pos[0] != '動詞'
                or not components[-2].lemma):
            continue
        base = ''.join(c.surface for c in components[:-2]) + components[-2].lemma
        candidate = candidates.get(base)
        if (candidate is not None and candidate.dictionary_expression_occurrences
                and candidate.frequency + candidate.reassigned_occurrences >= threshold):
            return True
    return False


def filter_useful_words(input: OrderedDict, min_frequency: int, keep_percent: int = 98, progress_handler=None, min_frequency_per_source: float = 1.1) -> OrderedDict:
    total = len(input)

    if total == 0:
        return OrderedDict()

    kept = OrderedDict()

    # Build sorted list of frequencies (no intermediate list)
    freqs = sorted(s.frequency for s in input.values())

    # Calculate threshold
    idx = max(0, int(total * (100 - keep_percent) / 100))
    threshold = max(freqs[idx], min_frequency)

    # Local bindings (hot path)
    kept_set = kept.__setitem__

    for i, (word, stats) in enumerate(input.items(), 1):
        if _is_pure_vocalization(word, stats):
            continue

        # Corrected imperative occurrences can keep a genuine potential candidate
        # eligible, but do not inflate its recorded frequency or examples.
        eligibility_frequency = stats.frequency + stats.reassigned_occurrences
        eligibility_tags = stats.tags | stats.reassigned_tags
        frequency_per_source = eligibility_frequency / len(eligibility_tags)

        if eligibility_frequency >= threshold and frequency_per_source >= min_frequency_per_source:
            kept_set(word, stats)
        elif ((eligibility_frequency >= threshold and stats.dictionary_expression_occurrences > 0)
              or _has_supported_base_expression(stats, input, threshold)):
            # Preserve dictionary-backed expressions for review when coverage is
            # low or a sufficiently frequent base expression supports the form.
            stats.coverage_review = True
            kept_set(word, stats)

        if progress_handler:
            progress_handler(
                i,
                total,
                f"{len(kept)} tokens filtered."
            )

    return kept

