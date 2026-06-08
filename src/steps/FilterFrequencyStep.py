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
    def __init__(self, min_frequency: int):
        self._processing_step = ProcessingStep.FILTERING
        self.min_frequency = min_frequency

    def process(self, artifact: Artifact) -> Artifact:
        data = filter_useful_words(artifact.data, min_frequency=self.min_frequency, progress_handler=self.progress)
        return Artifact(data, sentences=artifact.sentences)


def filter_useful_words(input: OrderedDict, min_frequency: int, keep_percent: int = 98, progress_handler=None) -> OrderedDict:
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
        if stats.frequency >= threshold:
            kept_set(word, stats)

        if progress_handler:
            progress_handler(
                i,
                total,
                f"{len(kept)} tokens filtered",
            )

    return kept

