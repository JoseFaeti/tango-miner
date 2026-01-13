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
        data = filter_useful_words(artifact.data, self.min_frequency, self.progress)
        return Artifact(data)


def filter_useful_words(input: OrderedDict, min_frequency: int, progress_handler=None) -> OrderedDict:
    total = len(input)
    kept = OrderedDict()

    for i, (word, stats) in enumerate(input.items(), 1):
        if stats.frequency >= max(min_frequency, int(math.log10(total) * min_frequency)):
            kept[word] = stats

        if progress_handler:
            progress_handler(ProcessingStep.FILTERING, i, total)

    return kept