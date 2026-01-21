from collections import OrderedDict
from pathlib import Path
import csv
import math
import re

from .Artifact import Artifact
from .Column import Column
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep


class FilterFrequencyStep(PipelineStep):
    def __init__(self, min_frequency: int):
        self.min_frequency = min_frequency

    def process(self, artifact: Artifact) -> Artifact:
        data = filter_useful_words(artifact.data, min_frequency=self.min_frequency, progress_handler=self.progress)
        return Artifact(data)


def filter_useful_words(input: OrderedDict, min_frequency: int, keep_percent: int = 98, progress_handler=None) -> OrderedDict:
    total = len(input)
    kept = OrderedDict()

    # Build sorted list of frequencies
    freqs = sorted([s.frequency for s in input.values()])
    if total == 0:
        threshold = min_frequency
    else:
        # Calculate index for percentile
        idx = max(0, int(total * (100 - keep_percent) / 100))
        threshold = max(freqs[idx], min_frequency)

    for i, (word, stats) in enumerate(input.items(), 1):
        if stats.frequency >= threshold:
            kept[word] = stats

        if progress_handler:
            progress_handler(ProcessingStep.FILTERING, i, total, f'{len(kept)} tokens filtered')

    return kept
