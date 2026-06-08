import math

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.steps.ProcessingStep import ProcessingStep


class ScoreWordStep(PipelineStep):
    def __init__(self):
        self._processing_step = ProcessingStep.SCORING


    def process(self, artifact: Artifact) -> Artifact:
        artifact.data = score_words(artifact.data, self.progress)
        return artifact


def score_words(input: dict, progress_handler=None) -> dict:
    total_words = len(input)

    if not total_words:
        return input

    max_frequency = max(stats.frequency for stats in input.values())

    for i, stats in enumerate(input.values(), 1):
        score = calculate_score(
            stats.index,
            stats.frequency,
            max_frequency
        )

        score *= tag_diversity_factor(len(stats.tags))

        stats.score = score

        if progress_handler:
            progress_handler(i, total_words) #, f"{score}: [index={stats.index}/{max_index}; freq={stats.frequency}/{max_frequency}]\n")

    # Normalize so the top word always scores 1000
    max_score = max(stats.score for stats in input.values())
    if max_score > 0:
        for stats in input.values():
            stats.score = round(stats.score / max_score * 1000, 2)

    return input


def calculate_score(
    index: float,
    frequency: int,
    max_frequency: int,
    w_freq: float = 0.7,
    w_index: float = 0.3
) -> float:
    """
    index: mean normalized position across files (0.0 = start, 1.0 = end)
    frequency: raw frequency of the word
    max_frequency: max frequency in corpus
    w_freq: weight for frequency
    w_index: weight for position
    """

    # index is already 0–1; invert so early words score higher
    index_score = (1 - index) ** 1.5

    # Log-scale frequency to spread out scores for common words
    frequency_score = math.log1p(frequency) / math.log1p(max_frequency) if max_frequency > 0 else 0

    # Weighted combination
    return w_freq * frequency_score + w_index * index_score


def tag_diversity_factor(tag_count: int, min_tags=3, saturation_tags=10) -> float:
    """
    Continuous soft penalty for tag coverage.
    1 tag → ~0.1
    min_tags → ~0.4–0.5
    saturation_tags → 1.0
    """
    if tag_count >= saturation_tags:
        return 1.0
    effective_count = max(tag_count, 1)
    min_factor = 0.1
    return min_factor + (1 - min_factor) * ((effective_count - 1) / (saturation_tags - 1))