"""Stream merged-token occurrences for later analysis without changing mining."""
import json
from dataclasses import asdict
from pathlib import Path

from src.PipelineStep import PipelineStep
from src.TokenProvenance import token_components
from src.steps.TokenizeStep import is_useless
from src.steps.ProcessingStep import ProcessingStep

PROGRESS_INTERVAL = 2000


class OccurrenceDiagnosticsStep(PipelineStep):
    def __init__(self, path: Path):
        self.path = Path(path)
        self._processing_step = ProcessingStep.OCCURRENCE_DIAGNOSTICS
        self.start_time = None

    def process(self, artifact):
        temporary = self.path.with_suffix(self.path.suffix + '.tmp')
        total = len(artifact.data)
        if total:
            self.progress(0, total, 'Writing occurrences.jsonl')
        with temporary.open('w', encoding='utf-8', newline='\n') as stream:
            for sentence_index, sentence in enumerate(artifact.data):
                cursor = 0
                for token_index, token in enumerate(sentence.tokens):
                    surface = token['surface']
                    start = sentence.text.find(surface, cursor) if cursor is not None and surface else -1
                    span = None
                    if surface and start >= 0 and not sentence.text[cursor:start].strip():
                        span = [start, start + len(surface)]
                        cursor = span[1]
                    elif surface:
                        cursor = None
                    lemma = token['base_form'] or token['lemma']
                    reading = token['reading']
                    eligible = bool(lemma and reading and not is_useless(token))
                    record = {
                        'sentence_index': sentence_index, 'token_index': token_index,
                        'source': str(sentence.origin) if sentence.origin is not None else None,
                        'tag': sentence.tag, 'sentence': sentence.text, 'span': span,
                        'candidate': lemma, 'surface': surface, 'reading': reading,
                        'pos': list(token['pos']), 'eligible_for_collection': eligible,
                        'components': [asdict(c) for c in token_components(token)],
                        'analysis_rule': token.get('analysis_rule', 'sudachi'),
                        'reassigned_from': token.get('reassigned_from'),
                    }
                    stream.write(json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n')
                completed = sentence_index + 1
                if completed % PROGRESS_INTERVAL == 0 and completed < total:
                    self.progress(completed, total, 'Writing occurrences.jsonl')
        temporary.replace(self.path)
        self.done(f'Occurrence diagnostics written to {self.path.name}.')
        return artifact
