from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep
from .SegmentedSentence import SegmentedSentence
from .WordStats import Sentence, WordStats

MAX_SENTENCES = 3

# Words whose scores are unknown (not yet in word_data) are treated as having
# this difficulty.  Set to 0 so unknown words make a sentence look harder,
# which is the conservative choice.
UNKNOWN_WORD_SCORE = 0.0


class AttachSentencesStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        attach_sentences(artifact.data, artifact.sentences, self.progress)
        return artifact


# ---------------------------------------------------------------------------
# Public function (also importable for testing)
# ---------------------------------------------------------------------------

def attach_sentences(
    word_data: dict,
    segmented_sentences: list[SegmentedSentence],
    progress_handler=None,
):
    """
    For every SegmentedSentence, try to attach it to each word it contains.
    Each word ends up with at most MAX_SENTENCES sentences, chosen to be as
    close as possible in difficulty to the word itself.

    Difficulty of a sentence is defined as the mean score of all the scored
    words it contains. A sentence is a better fit for a word the smaller the
    absolute difference between the sentence difficulty and the word's own score.
    """
    total = len(segmented_sentences)

    # print(f'Total segmented sentences: {total}')

    for i, seg in enumerate(segmented_sentences):
        if progress_handler:
            progress_handler(ProcessingStep.SENTENCES, i, total)

        sentence_difficulty = _sentence_difficulty(seg, word_data)

        for lemma, surface in seg.lemma_surfaces.items():
            ws = word_data.get(lemma)
            if ws is None:
                continue

            candidate = Sentence(
                text=seg.text,
                tag=seg.tag,
                origin=seg.origin,
                surface_form=surface,
            )

            _try_attach(ws, candidate, sentence_difficulty)

    if progress_handler:
        progress_handler(ProcessingStep.SENTENCES, total, total)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sentence_difficulty(seg: SegmentedSentence, word_data: dict) -> float:
    """
    Mean score of the known words in this sentence.
    Words not present in word_data are treated as UNKNOWN_WORD_SCORE.
    """
    scores = []
    for lemma in seg.lemma_surfaces:
        ws = word_data.get(lemma)
        score = ws.score if ws is not None else UNKNOWN_WORD_SCORE
        scores.append(score)

    if not scores:
        return UNKNOWN_WORD_SCORE

    return sum(scores) / len(scores)


def _difficulty_delta(word_score: float, sentence_difficulty: float) -> float:
    """How far the sentence difficulty is from the target word's score."""
    return abs(word_score - sentence_difficulty)


def _sentence_fitness(ws: WordStats, candidate: Sentence, sentence_difficulty: float) -> float:
    """
    A lower value means a better fit.
    We use the absolute difficulty delta as the primary criterion, and
    sentence length as a tiebreaker (shorter is better).
    """
    delta = _difficulty_delta(ws.score, sentence_difficulty)
    length_penalty = len(candidate.text) / 1000
    return delta + length_penalty


def _already_attached(ws: WordStats, text: str) -> bool:
    return any(s.text == text for s in ws.sentences)


def _try_attach(ws: WordStats, candidate: Sentence, sentence_difficulty: float):
    """
    Attach candidate to ws if it fits better than what is already there.

    - If there are fewer than MAX_SENTENCES, always attach.
    - Otherwise replace the worst-fitting existing sentence if the candidate
      is a better fit.
    """
    if _already_attached(ws, candidate.text):
        return

    if len(ws.sentences) < MAX_SENTENCES:
        ws.sentences.append(candidate)
        return

    # Find the currently worst-fitting sentence
    worst_sentence = None
    worst_fitness = -1.0

    for existing in ws.sentences:
        existing_difficulty = _sentence_difficulty_from_sentence(existing, ws)
        fitness = _sentence_fitness(ws, existing, existing_difficulty)
        if fitness > worst_fitness:
            worst_fitness = fitness
            worst_sentence = existing

    candidate_fitness = _sentence_fitness(ws, candidate, sentence_difficulty)

    if candidate_fitness < worst_fitness:
        ws.sentences.remove(worst_sentence)
        ws.sentences.append(candidate)


def _sentence_difficulty_from_sentence(sentence: Sentence, ws: WordStats) -> float:
    """
    Approximate difficulty of an already-attached Sentence.
    Since we no longer have the full lemma_surfaces mapping for attached
    sentences, we use the word's own score as a proxy.  This is reasonable
    because the sentence was attached because it was a good fit for the word.
    """
    return ws.score