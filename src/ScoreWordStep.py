from collections import OrderedDict

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep


class ScoreWordStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        data = score_words(artifact.data, self.progress)
        return Artifact(data)


def score_words(input: OrderedDict, progress_handler=None) -> OrderedDict:
    total_words = len(input)

    max_frequency = max(stats.frequency for stats in input.values())
    max_index = max(stats.index for stats in input.values())

    for i, stats in enumerate(input.values(), 1):
        score = calculate_score(
            stats.index,
            max_index,
            stats.frequency,
            max_frequency
        )

        stats.score = round(score * 1000, 2)

        if progress_handler:
            progress_handler(ProcessingStep.SCORING, i, total_words)

    return input


def calculate_score(
    index: int,
    max_index: int,
    frequency: int,
    max_frequency: int,
    w_freq: float = 0.7,
    w_index: float = 0.3
) -> float:
    index_score = 1 - (index / max_index)
    frequency_score = frequency / max_frequency

    return w_freq * frequency_score + w_index * index_score
