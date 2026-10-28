"""Write optional grammar proposals without modifying the token stream."""
import json
from pathlib import Path

from src.GrammarShadow import analyze_sentence, fingerprint
from src.PipelineStep import PipelineStep
from src.steps.ProcessingStep import ProcessingStep


class GrammarShadowStep(PipelineStep):
    def __init__(self, path):
        self.path = Path(path)
        self._processing_step = getattr(ProcessingStep, 'GRAMMAR_SHADOW', ProcessingStep.OCCURRENCE_DIAGNOSTICS)
        self.start_time = None

    def process(self, artifact):
        temporary = self.path.with_suffix(self.path.suffix + '.tmp')
        count = 0
        rule_hash = fingerprint()
        with temporary.open('w', encoding='utf-8', newline='\n') as stream:
            for index, sentence in enumerate(artifact.data):
                row = analyze_sentence(sentence, index)
                row['rule_sha256'] = rule_hash
                count += len(row['proposals'])
                stream.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
                if (index + 1) % 2000 == 0 and index + 1 < len(artifact.data):
                    self.progress(index + 1, len(artifact.data))
        temporary.replace(self.path)
        self.done(f'{count} tentative grammar proposals written to {self.path.name}.')
        return artifact
