import appdirs
from pathlib import Path

from src.Artifact import Artifact
from src.JMDict import JMDict
from src.PipelineStep import PipelineStep
from src.steps.ProcessingStep import ProcessingStep


class AddDefinitionsStep(PipelineStep):
    def __init__(self, on_definition_processed=None, debug=False):
        self._processing_step = ProcessingStep.DEFINITIONS
        self.on_definition_processed = on_definition_processed
        self._debug = debug


    def process(self, artifact: Artifact, debug=False) -> Artifact:
        data, total_valid, total_invalid = add_and_filter_for_definitions(artifact.data, self.progress, self.on_definition_processed)

        if self._debug:
            dump_invalid_words(data)

        self.done(f'{total_valid} definitions found')

        return Artifact(data, sentences=artifact.sentences)


def add_and_filter_for_definitions(input: dict, progress_handler, on_definition_processed):
    total = len(input)
    kept = {}
    total_valid = 0
    total_invalid = 0

    progress_handler(0, total, 'Initializing dictionary...')
    
    jmdict = JMDict(Path.home() / "JMdict_e.xml")

    # print(f'total words to process: {total}')
    # print(f'total words to look up: {len(words_to_lookup)}')
    # print(f'total cached words: {len(cached_results)}')

    for i, word in enumerate(input):
        definition = jmdict.get_most_common_definition(word)
        
        if definition:
            total_valid += 1
        else:
            total_invalid += 1

        if on_definition_processed:
            on_definition_processed(word, definition)

        progress_handler(i, total, f'{i}/{total} {total_valid} definitions found')

        stats = input[word]
        stats.definition = definition
        stats.invalid = not definition
        kept[word] = stats

    return kept, total_valid, total_invalid


def dump_invalid_words(word_data: dict):
    base_dir = Path(appdirs.user_cache_dir("tango_miner")) / "debug"
    base_dir.mkdir(parents=True, exist_ok=True)

    output_path = base_dir / "invalid_words.txt"

    with output_path.open("w", encoding="utf-8") as f:
        for word, stats in word_data.items():
            if stats.invalid:
                f.write(f"{word}\t{stats.reading}\t{' '.join(sorted(stats.tags))}\n")

    print(f"Invalid words dumped to {output_path}")