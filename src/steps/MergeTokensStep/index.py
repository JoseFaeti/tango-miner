from contextlib import nullcontext
from pathlib import Path
import json

from src.Artifact import Artifact
from src.JMDict import JMDict
from src.PipelineStep import PipelineStep
from src.RunConfig import resolve_config
from src.TokenizedSentence import TokenizedSentence
from src.steps.ProcessingStep.index import ProcessingStep


PROGRESS_INTERVAL = 200

from .helpers import _is_split_kana_noun, _is_contracted_kana_verb, has_kanji
from .grammar_guards import _is_inflection_misread_as_noun
from .recovery import _repair_kigahen_boundaries, _repair_chigee_boundary, _repair_warui_koto_boundary
from .merger import combine_tokens_jmdict


PROGRESS_INTERVAL = 200

_jmdict = None
_jmdict_path = None


def get_jmdict(path: str | Path | None = None) -> JMDict:
    """Load the requested dictionary, or the default configuration's dictionary."""
    global _jmdict, _jmdict_path
    if path is None:
        config_path = Path("config.toml")
        path = resolve_config(config_path if config_path.is_file() else None).dictionary.jmdict_path
    resolved_path = Path(path).expanduser().resolve()
    if _jmdict is None or _jmdict_path != resolved_path:
        dictionary = JMDict(resolved_path)
        _jmdict, _jmdict_path = dictionary, resolved_path
    return _jmdict


class MergeTokensStep(PipelineStep):
    """Merge adjacent Sudachi tokens when their sequence is a JMdict entry."""

    def __init__(self, max_lookahead: int = 10, dictionary_path: str | Path | None = None, diagnostics_path: Path | None = None):
        self.diagnostics_path = diagnostics_path
        self._processing_step = ProcessingStep.TOKEN_MERGING
        self.max_lookahead = max_lookahead
        self.dictionary_path = dictionary_path

    def process(self, artifact: Artifact) -> Artifact:
        tokenized_sentences: list[TokenizedSentence] = artifact.data
        merged_sentences = []
        executor = getattr(self, '_parallel_executor', None)
        if executor is not None and len(tokenized_sentences) >= 512:
            from src.Parallel import batches, merge_batch, ordered_map
            batch_size = max(128, min(1000, (len(tokenized_sentences) + self._parallel_workers - 1)
                                       // self._parallel_workers))
            arguments = ((group, self.dictionary_path, self.max_lookahead, self.diagnostics_path is not None)
                         for _, group in batches(tokenized_sentences, batch_size))
            batch_results = ordered_map(executor, merge_batch, arguments, self._parallel_workers)
            packed_results = (result for group in batch_results for result in group)
            results = ((list(sentence.tokens) if packed is None else
                        [sentence.tokens[item] if type(item) is int else item for item in packed], rejected)
                       for sentence, (packed, rejected) in zip(tokenized_sentences, packed_results))
        else:
            jmdict = get_jmdict(self.dictionary_path)
            def serial_results():
                for sentence in tokenized_sentences:
                    rejected = [] if self.diagnostics_path is not None else None
                    yield (combine_tokens_jmdict(sentence.tokens, jmdict,
                                                max_lookahead=self.max_lookahead, rejected=rejected), rejected)
            results = serial_results()

        temporary = Path(self.diagnostics_path).with_suffix(".jsonl.tmp") if self.diagnostics_path is not None else None
        output = temporary.open("w", encoding="utf-8", newline="\n") if temporary is not None else nullcontext()
        with output as stream:
            for index, (sentence, (merged, sentence_events)) in enumerate(zip(tokenized_sentences, results), 1):
                merged_sentences.append(
                    TokenizedSentence(
                        text=sentence.text,
                        tag=sentence.tag,
                        origin=sentence.origin,
                        tokens=merged,
                    )
                )
                if sentence_events:
                    for event in sentence_events:
                        event.update(sentence_index=index - 1, sentence=sentence.text, source=str(sentence.origin) if sentence.origin is not None else None)
                        stream.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
                if index % PROGRESS_INTERVAL == 0 and index < len(tokenized_sentences):
                    self.progress(index, len(tokenized_sentences))

        if temporary is not None:
            temporary.replace(self.diagnostics_path)
        self.done(f"{len(merged_sentences)} token streams merged.")
        return Artifact(merged_sentences)
