"""Bounded, ordered process work for grammar and token merging."""
from collections import deque
from concurrent.futures import ProcessPoolExecutor
import hashlib
from pathlib import Path
import warnings



class ParallelPool:
    """Start helpers lazily, optionally sharing an immutable dictionary index."""
    def __init__(self, workers, cache_dir=None, dictionary_hash=None):
        self.workers = workers
        self.cache_dir = cache_dir
        self.dictionary_hash = dictionary_hash
        self.executor = None

    def _shared_initializer(self, function, arguments):
        if self.cache_dir is None or not isinstance(self.dictionary_hash, str):
            return None, ()
        try:
            digest = bytes.fromhex(self.dictionary_hash)
            if len(digest) != 32:
                return None, ()
            dictionary_path = arguments[2] if function is grammar_batch else arguments[1]
            if dictionary_path is None:
                return None, ()
            dictionary_path = Path(dictionary_path).expanduser().resolve()
            with dictionary_path.open('rb') as stream:
                actual = hashlib.file_digest(stream, 'sha256').hexdigest()
            if actual != self.dictionary_hash.lower():
                return None, ()
            from src.SharedJMDict import prepare_shared_index, install_worker_index
            index_path = prepare_shared_index(dictionary_path, self.dictionary_hash, self.cache_dir)
            return install_worker_index, (index_path, dictionary_path, self.dictionary_hash)
        except Exception as exc:
            warnings.warn(f'Shared JMdict index unavailable; workers will load separate dictionaries: {exc}',
                          RuntimeWarning)
            return None, ()

    def submit(self, function, arguments):
        if self.executor is None:
            initializer, initargs = self._shared_initializer(function, arguments)
            self.executor = ProcessPoolExecutor(max_workers=self.workers - 1,
                                                initializer=initializer, initargs=initargs)
        return self.executor.submit(function, arguments)

    def shutdown(self, **kwargs):
        if self.executor is not None:
            self.executor.shutdown(**kwargs)


def batches(items, size=1000):
    for start in range(0, len(items), size):
        yield start, items[start:start + size]


def ordered_map(executor, function, arguments, workers):
    """Run one share locally, with a bounded number of ordered worker tasks."""
    pending = deque()
    arguments = iter(arguments)
    for _ in range(workers - 1):
        try:
            pending.append((True, executor.submit(function, next(arguments))))
        except StopIteration:
            break
    while True:
        try:
            local = next(arguments)
        except StopIteration:
            break
        pending.append((False, function(local)))
        for index in range(workers):
            if not pending:
                break
            remote, value = pending.popleft()
            yield value.result() if remote else value
            if index < workers - 1:
                try:
                    pending.append((True, executor.submit(function, next(arguments))))
                except StopIteration:
                    pass
    while pending:
        remote, value = pending.popleft()
        yield value.result() if remote else value


def grammar_batch(args):
    from src.Artifact import Artifact
    from src.Grammar import GrammarStep

    start, sentences, dictionary_path, prefer_expressions, max_lookahead, max_examples = args
    stage = GrammarStep(max_examples=max_examples, dictionary_path=dictionary_path,
                        prefer_expressions=prefer_expressions, max_lookahead=max_lookahead,
                        profiling=False)
    stage.process(Artifact(sentences))
    occurrences = [(rule_id, group.count, group.tags, group.examples)
                   for rule_id, group in stage.occurrences.items() if group.count]
    return start, len(sentences), occurrences, stage.spans


def merge_batch(args):
    from src.steps.MergeTokensStep.index import combine_tokens_jmdict, get_jmdict

    sentences, dictionary_path, max_lookahead, diagnostics = args
    dictionary = get_jmdict(dictionary_path)
    results = []
    for sentence in sentences:
        rejected = [] if diagnostics else None
        merged = combine_tokens_jmdict(sentence.tokens, dictionary,
                                       max_lookahead=max_lookahead, rejected=rejected)
        if len(merged) == len(sentence.tokens) and all(
                left is right for left, right in zip(merged, sentence.tokens)):
            packed = None
        else:
            positions = {id(token): index for index, token in enumerate(sentence.tokens)}
            packed = [positions[id(token)] if id(token) in positions else token for token in merged]
        results.append((packed, rejected))
    return results
