from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.SegmentedSentence import SegmentedSentence
from src.TokenizedSentence import TokenizedSentence
from src.TokenProvenance import token_components
from src.WordStats import WordStats
from src.steps.ProcessingStep import ProcessingStep
from src.steps.TokenizeStep import is_useless, kata_to_hira, is_valid_sentence


PROGRESS_INTERVAL = 200


class BuildWordStatsStep(PipelineStep):
    """Build word statistics and sentence mappings from merged token streams."""

    def __init__(self):
        self._processing_step = ProcessingStep.WORD_COLLECTION

    def process(self, artifact: Artifact) -> Artifact:
        word_data, segmented_sentences = build_word_stats(
            artifact.data,
            progress_handler=self.progress,
            progress_interval=PROGRESS_INTERVAL,
        )
        self.done(
            f"{len(word_data)} tokens and {len(segmented_sentences)} sentences collected."
        )
        return Artifact(word_data, sentences=segmented_sentences)


def build_word_stats(
    tokenized_sentences: list[TokenizedSentence],
    progress_handler=None,
    word_data=None,
    segmented_sentences=None,
    progress_interval: int = 200,
):
    """Aggregate merged tokens while retaining per-source index positions."""
    word_data = word_data or {}
    segmented_out = list(segmented_sentences) if segmented_sentences else []
    current_origin = None
    lemma_first_pos_in_file: dict[str, int] = {}
    token_index = 0

    def update_source_indexes():
        if token_index == 0:
            return
        for lemma, first_pos in lemma_first_pos_in_file.items():
            ws = word_data[lemma]
            file_norm_pos = first_pos / token_index
            ws.index = (ws.index * ws.index_count + file_norm_pos) / (ws.index_count + 1)
            ws.index_count += 1

    for index, tokenized_sentence in enumerate(tokenized_sentences, 1):
        if current_origin is not None and tokenized_sentence.origin != current_origin:
            update_source_indexes()
            lemma_first_pos_in_file = {}
            token_index = 0
        current_origin = tokenized_sentence.origin

        sentence_surfaces = {}
        for token in tokenized_sentence.tokens:
            lemma = token["base_form"] or token["lemma"]
            reading = token["reading"]
            if not lemma or not reading or is_useless(token):
                continue

            token_index += 1
            lemma_first_pos_in_file.setdefault(lemma, token_index)
            sentence_surfaces[lemma] = token["surface"]

            ws = word_data.get(lemma)
            if ws:
                ws.frequency += 1
            else:
                ws = WordStats(
                    0.0, 0, 1, 0.0, kata_to_hira(reading), "", set(), [],
                    lemma, token["pos"], invalid=False,
                )
                word_data[lemma] = ws

            components = token_components(token)
            ws.component_counts[components] = ws.component_counts.get(components, 0) + 1

            if tokenized_sentence.tag:
                ws.tags.add(tokenized_sentence.tag)

        if is_valid_sentence(tokenized_sentence.text):
            segmented_out.append(
                SegmentedSentence(
                    text=tokenized_sentence.text,
                    tag=tokenized_sentence.tag,
                    origin=None,
                    lemma_surfaces=sentence_surfaces,
                )
            )

        if progress_handler and index % progress_interval == 0 and index < len(tokenized_sentences):
            progress_handler(index, len(tokenized_sentences))

    update_source_indexes()
    return word_data, segmented_out