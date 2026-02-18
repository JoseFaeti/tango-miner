from pathlib import Path

from .Artifact import Artifact
from .JMDict import JMDict
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep


class AddDefinitionsStep(PipelineStep):
    def __init__(self, on_definition_processed=None):
        self.on_definition_processed = on_definition_processed

    def process(self, artifact: Artifact) -> Artifact:
        data = add_and_filter_for_definitions(artifact.data, self.progress, self.on_definition_processed)
        return Artifact(data)


def add_and_filter_for_definitions(input: dict, progress_handler, on_definition_processed):
    total = len(input)
    kept = {}
    total_valid = 0
    total_invalid = 0

    progress_handler(ProcessingStep.DEFINITIONS, 0, total, 'Initializing dictionary...')
    
    jmdict = JMDict(Path.home() / "JMdict_e.xml")

    # print(f'total words to process: {total}')
    # print(f'total words to look up: {len(words_to_lookup)}')
    # print(f'total cached words: {len(cached_results)}')

    found = 0

    for i, word in enumerate(input):
        result = jmdict.get_most_common_definition(word)
        
        if result:
            found += 1

        if on_definition_processed:
            on_definition_processed(word, result)

        progress_handler(
            ProcessingStep.DEFINITIONS,
            i,
            total,
            f'{i}/{total} {found} definitions found'
        )

        definition = result
        stats = input[word]
        stats.definition = definition
        stats.invalid = not definition
        kept[word] = stats

        if definition:
            total_valid += 1
        else:
            total_invalid += 1

    progress_handler(
        ProcessingStep.DEFINITIONS,
        1,
        1,
        f'{len(kept) - total_invalid} definitions found'
    )

    # print(f'total valid: {total_valid}')
    # print(f'total invalid: {total_invalid}')

    return kept
