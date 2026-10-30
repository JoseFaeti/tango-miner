"""Cache the state produced by the registered grammar analysis stage."""
import hashlib
import json
import sys
import tempfile
from time import perf_counter
from dataclasses import asdict
from pathlib import Path

from src.Grammar import GrammarOccurrences, GrammarStep
from src.WordStats import Sentence
from src.grammar_rules.registry import RULES
from src.StageCache import _bytes, _input_key_data, MERGE_SOURCE_DEPENDENCIES


def _fingerprint():
    root = Path(__file__).resolve().parent
    names = set(MERGE_SOURCE_DEPENDENCIES) | {
        'GrammarCache.py', 'Grammar.py', 'GrammarShadow.py', 'WordStats.py',
        'steps/TokenizeStep.py',
    }
    names.update(str(path.relative_to(root)).replace('\\', '/')
                 for path in (root / 'grammar_rules').rglob('*.py'))
    digest = hashlib.sha256()
    for name in sorted(names):
        digest.update(name.encode('utf-8') + b'\0' + (root / name).read_bytes() + b'\0')
    return digest.hexdigest()


def _encode(step):
    return {
        'occurrences': {key: {'count': group.count, 'tags': sorted(group.tags),
                              'examples': [asdict(example) for example in group.examples]}
                        for key, group in step.occurrences.items()},
        'spans': [[index, spans] for index, spans in step.spans.items()],
    }


def _restore(step, data, sentences):
    groups = data['occurrences']
    if set(groups) != {rule.id for rule in step.rules}:
        raise ValueError('Invalid cached rules')
    occurrences = {}
    for key, group in groups.items():
        if type(group['count']) is not int or group['count'] < 0:
            raise ValueError('Invalid cached count')
        examples = [Sentence(**{**example, 'span': tuple(example['span']) if example['span'] is not None else None})
                    for example in group['examples']]
        occurrences[key] = GrammarOccurrences(count=group['count'], tags=set(group['tags']),
                                               examples=examples, example_texts={e.text for e in examples})
    spans = {}
    matches = {}
    for index, items in data['spans']:
        if type(index) is not int or not 0 <= index < len(sentences) or index in spans:
            raise ValueError('Invalid cached sentence index')
        restored = []
        for rule_id, start, end in items:
            if rule_id not in occurrences or type(start) is not int or type(end) is not int or not 0 <= start < end <= len(sentences[index].text):
                raise ValueError('Invalid cached span')
            restored.append((rule_id, start, end))
        spans[index] = restored
        matches[index] = [(rule_id, start) for rule_id, start, _ in restored]
    step.occurrences, step.spans, step.matches = occurrences, spans, matches


class GrammarCache:
    def __init__(self, directory, dictionary_hash, refresh):
        self.directory = Path(directory) / 'stages' / 'grammar-v1'
        self.dictionary_hash = dictionary_hash
        self.refresh = set(refresh)

    def process(self, step, artifact):
        stats = step.cache_stats = {'stage': 'grammar', 'unit': 'corpus', 'hits': 0,
                                    'misses': 0, 'refreshes': 0, 'write_failures': 0}
        step.progress(0, 1, 'Preparing grammar cache')
        step.performance = {'per_rule_requested': step.profile_rules}
        step._performance_entries = {}
        clock = perf_counter if step._performance_enabled else lambda: 0.0
        started = clock()
        try:
            if len(step.rules) != len(RULES) or any(a is not b for a, b in zip(step.rules, RULES)):
                raise ValueError('custom rules')
            if step.prefer_expressions and not self.dictionary_hash:
                raise ValueError('dictionary hash unavailable')
            key_data = ['grammar-v1', sys.version, _fingerprint(),
                        self.dictionary_hash if step.prefer_expressions else None,
                        step.prefer_expressions, step.max_lookahead, step.max_examples,
                        _input_key_data(artifact.data, progress=lambda done, total: step.progress(done, total, 'Preparing cache key'))]
            step.progress(0, 1, 'Hashing cache key')
            # ASCII escaping lets the JSON encoder produce bytes more cheaply
            # on Japanese text; it preserves the complete key data exactly.
            key_bytes = json.dumps(key_data, ensure_ascii=True, separators=(',', ':'),
                                   allow_nan=False).encode('ascii')
            key = hashlib.sha256(key_bytes).hexdigest()
        except (OSError, TypeError, ValueError, KeyError, AttributeError, RecursionError) as exc:
            stats['disabled_reason'] = str(exc) or 'cache key unavailable'
            key = None
        finally:
            step.add_performance_time('cache_key', clock() - started)
        if key is None:
            return step.process(artifact)
        path = self.directory / (key + '.json')
        forced = bool(self.refresh & {'grammar', 'all'})
        if not forced:
            step.progress(0, 1, 'Checking grammar cache')
            started = clock()
            try:
                entry = json.loads(path.read_text(encoding='utf-8'))
                if entry['key'] != key or entry['checksum'] != hashlib.sha256(_bytes(entry['data'])).hexdigest():
                    raise ValueError('Invalid cache checksum')
                _restore(step, entry['data'], artifact.data)
            except (OSError, ValueError, TypeError, KeyError, IndexError, AttributeError, RecursionError):
                step.add_performance_time('cache_read', clock() - started)
            else:
                step.add_performance_time('cache_read', clock() - started)
                stats['hits'] = 1
                step.done(f'{sum(g.count for g in step.occurrences.values())} source-supported grammar occurrences. Cache hit.')
                return artifact
        stats['refreshes' if forced else 'misses'] = 1
        step.progress(0, 1, 'Matching grammar')
        result = step.process(artifact)
        temporary = None
        step.progress(0, 1, 'Saving grammar cache')
        started = clock()
        try:
            data = _encode(step)
            entry = {'key': key, 'checksum': hashlib.sha256(_bytes(data)).hexdigest(), 'data': data}
            self.directory.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=self.directory, delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(_bytes(entry))
            temporary.replace(path)
        except (OSError, TypeError, ValueError, KeyError, AttributeError, RecursionError):
            stats['write_failures'] += 1
        finally:
            step.add_performance_time('cache_write', clock() - started)
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass
        return result
