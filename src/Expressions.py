"""Match a curated inventory against aligned Sudachi token streams."""
from dataclasses import asdict, dataclass
from functools import lru_cache
from itertools import islice
import json
from pathlib import Path

from src.grammar_rules.alignment import aligned_tokens
from src.TokenProvenance import token_components
from src.PipelineStep import PipelineStep
from src.WordStats import WordStats
from src.steps.ProcessingStep.index import ProcessingStep
from src.steps.TokenizeStep.index import is_useless


INVENTORY_PATH = Path(__file__).resolve().parent.parent / 'data' / 'expressions.json'


@lru_cache(maxsize=1)
def inventory():
    with INVENTORY_PATH.open(encoding='utf-8') as stream:
        data = json.load(stream)
    return validate_inventory(data)


def validate_inventory(data):
    if not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('entries'), list):
        raise ValueError('Unsupported expression inventory')
    entries = data['entries']
    seen = set()
    for entry in entries:
        scalar_keys = ('expression', 'reading', 'particle', 'meaning', 'reference')
        if (not isinstance(entry, dict)
                or not all(isinstance(entry.get(key), str) and entry[key] for key in scalar_keys)
                or not all(isinstance(entry.get(key), list) and entry[key]
                           and all(isinstance(item, str) and item for item in entry[key])
                           for key in ('noun', 'verb'))
                or entry.get('verb_class') not in {'ichidan', 'godan', 'i_adjective'}
                or entry['expression'] in seen
                or any(not verb.endswith('る') for verb in entry['verb']) and entry['verb_class'] == 'ichidan'
                or any(verb[-1] not in 'うくぐすつぬぶむる' for verb in entry['verb']) and entry['verb_class'] == 'godan'
                or any(not verb.endswith('い') for verb in entry['verb']) and entry['verb_class'] == 'i_adjective'):
            raise ValueError('Invalid or duplicate expression inventory entry')
        seen.add(entry['expression'])
    return tuple(entries)


@dataclass(frozen=True)
class ExpressionMatch:
    entry: dict
    start: int
    end: int
    components: tuple
    omitted_particle: bool
    analysis_rule: str = 'sudachi'


def _verb_forms(verb, verb_class):
    # Sudachi may use a separate dictionary lemma for a godan potential or
    # contracted causative, rather than exposing the original verb lemma.
    if verb_class == 'i_adjective':
        return {verb}
    rows = {'う': ('え', 'わ'), 'く': ('け', 'か'), 'ぐ': ('げ', 'が'),
            'す': ('せ', 'さ'), 'つ': ('て', 'た'), 'ぬ': ('ね', 'な'),
            'ぶ': ('べ', 'ば'), 'む': ('め', 'ま'), 'る': ('れ', 'ら')}
    if verb_class == 'ichidan' and verb.endswith('る'):
        return {verb, verb[:-1] + 'れる', verb[:-1] + 'さす'}
    if verb[-1:] not in rows:
        return {verb}
    potential, causative = rows[verb[-1]]
    return {verb, verb[:-1] + potential + 'る', verb[:-1] + causative + 'す'}


@lru_cache(maxsize=256)
def _surface_forms(verb, verb_class):
    """Finite forms for recovering an analyzer's merged or misread tokens."""
    stem = verb[:-1]
    if verb_class == 'i_adjective':
        return {verb, *(stem + suffix for suffix in (
            'く', 'かった', 'ければ', 'くて', 'くない', 'くなかった',
            'くなければ', 'くなくて', 'くありません', 'くありませんでした', 'くなきゃ'))}
    if verb_class == 'ichidan':
        negative, polite, conditional, volitional, imperative = stem, stem, stem + 'れ', stem + 'よう', stem + 'ろ'
        past, connective = stem + 'た', stem + 'て'
        passive, causative, potential = stem + 'られ', stem + 'させ', stem + 'れ'
    else:
        rows = {'う': ('わ', 'い', 'え', 'お', 'った', 'って'),
                'く': ('か', 'き', 'け', 'こ', 'いた', 'いて'),
                'ぐ': ('が', 'ぎ', 'げ', 'ご', 'いだ', 'いで'),
                'す': ('さ', 'し', 'せ', 'そ', 'した', 'して'),
                'つ': ('た', 'ち', 'て', 'と', 'った', 'って'),
                'ぬ': ('な', 'に', 'ね', 'の', 'んだ', 'んで'),
                'ぶ': ('ば', 'び', 'べ', 'ぼ', 'んだ', 'んで'),
                'む': ('ま', 'み', 'め', 'も', 'んだ', 'んで'),
                'る': ('ら', 'り', 'れ', 'ろ', 'った', 'って')}
        if verb[-1:] not in rows:
            return {verb}
        a, i, e, o, past_end, te_end = rows[verb[-1]]
        negative, polite, conditional = stem + a, stem + i, stem + e
        volitional, imperative = stem + o + 'う', stem + e
        past, connective = stem + past_end, stem + te_end
        passive, causative, potential = stem + a + 'れ', stem + a + 'せ', stem + e
    forms = {verb, polite, negative + 'ない', negative + 'なかった', negative + 'ず',
             polite + 'ます', polite + 'ました', polite + 'ません', polite + 'ませんでした',
             conditional + 'ば', volitional, imperative, past, connective}
    forms.update(negative + suffix for suffix in ('なければ', 'なかったら', 'なくて', 'なく', 'ないで', 'ぬ', 'ん', 'なきゃ'))
    forms.update(polite + suffix for suffix in ('ましょう', 'たい', 'たかった', 'たくない', 'たければ'))
    forms.update((past + 'ら', past + 'り', connective + 'いる', connective + 'いた'))
    forms.update(connective + suffix for suffix in ('る', 'た', 'ない', 'おく', 'しまう', 'しまった'))
    contracted = connective[:-1] + ('じゃ' if connective.endswith('で') else 'ちゃ')
    forms.update(contracted + suffix for suffix in ('う', 'った', 'わない', 'います'))
    preparation = connective[:-1] + ('ど' if connective.endswith('で') else 'と')
    forms.update(preparation + suffix for suffix in ('く', 'いた', 'かない'))
    for base in (passive, causative, potential):
        forms.update(base + suffix for suffix in ('る', 'ない', 'なかった', 'た', 'て', 'ます', 'ました', 'れば', 'よう'))
    forms.add(causative + 'られる')
    return forms


def _reanalyze_end(text, start, prefix, entry, boundaries):
    """Reanalyze a verb after a merged noun, retaining only raw-token boundaries."""
    if not text.startswith(prefix, start):
        return None
    offset = start + len(prefix)
    fragment = text[offset:]
    if not any(fragment.startswith(verb[:-1]) for verb in entry['verb']):
        return None
    from src.steps.TokenizeStep.index import get_tokenizer, TOKENIZER_MODE
    nodes = get_tokenizer().tokenize(fragment, TOKENIZER_MODE)
    if not nodes:
        return None
    allowed = {form for verb in entry['verb'] for form in _verb_forms(verb, entry['verb_class'])}
    first = nodes[0]
    expected_pos = '形容詞' if entry['verb_class'] == 'i_adjective' else '動詞'
    if first.part_of_speech()[0] != expected_pos or first.dictionary_form() not in allowed:
        return None
    end = offset + len(first.surface())
    previous_surface = first.surface()
    for node in islice(nodes, 1, None):
        pos = node.part_of_speech()
        if (expected_pos == '形容詞' and previous_surface.endswith('く')
                and pos[0] == '形容詞' and node.dictionary_form() == 'ない'):
            end += len(node.surface())
        elif pos[0] == '助動詞':
            end += len(node.surface())
        elif pos[:2] == ('助詞', '接続助詞') and node.surface() in {'て', 'で', 'ば'}:
            end += len(node.surface())
            break
        else:
            break
        previous_surface = node.surface()
    return end if end in boundaries else None


def _particle_variants(particle):
    if particle in {'が', 'を'}:
        return (particle, 'は', 'も', '')
    if particle == 'に':
        return (particle + 'は', particle + 'も', particle, '')
    return (particle, '')


def _recover_surface_matches(sentence, aligned, entries, found):
    """Recover known inflections at raw-token boundaries when tokenization fails."""
    boundaries = {point for token in aligned for point in token['span']}
    starts = {token['span'][0] for token in aligned}
    existing = {(match.start, match.entry['expression']) for match in found}
    for entry in entries:
        for start in starts:
            if (start, entry['expression']) in existing:
                continue
            for noun in entry['noun']:
                if not sentence.text.startswith(noun, start):
                    continue
                forms = {form for verb in entry['verb'] for form in _surface_forms(verb, entry['verb_class'])}
                for particle in _particle_variants(entry['particle']):
                    prefix = noun + particle
                    hits = [start + len(prefix) + len(form) for form in forms
                            if sentence.text.startswith(prefix + form, start)
                            and start + len(prefix) + len(form) in boundaries]
                    recovered_end = max(hits, default=None)
                    analyzed_end = _reanalyze_end(sentence.text, start, prefix, entry, boundaries)
                    if recovered_end is None and analyzed_end is None:
                        continue
                    if analyzed_end is not None and (recovered_end is None or analyzed_end > recovered_end):
                        end, rule = analyzed_end, 'verb_reanalysis'
                    else:
                        end, rule = recovered_end, 'surface_recovery'
                    components = tuple(component for token in aligned
                                       if start <= token['span'][0] and token['span'][1] <= end
                                       for component in token_components(sentence.tokens[token['index']]))
                    found.append(ExpressionMatch(entry, start, end, components, not particle, rule))
                    existing.add((start, entry['expression']))
                    break
                if (start, entry['expression']) in existing:
                    break
    return found


def find_expressions(sentence, entries=None):
    """Return complete, source-aligned matches without consuming lexical tokens."""
    entries = inventory() if entries is None else entries
    aligned, error = aligned_tokens(sentence.text, sentence.tokens)
    if error:
        return []
    raw = sentence.tokens
    found = []
    for i, first in enumerate(aligned):
        for entry in entries:
            if first['surface'] not in entry['noun'] or not first['pos'] or first['pos'][0] != '名詞':
                continue
            j = i + 1
            if j >= len(aligned) or first['span'][1] != aligned[j]['span'][0]:
                continue
            particle = next((variant for variant in _particle_variants(entry['particle'])
                             if variant and sentence.text.startswith(variant, first['span'][1])), '')
            omitted = not particle
            particle_end = first['span'][1] + len(particle)
            while j < len(aligned) and aligned[j]['span'][0] < particle_end:
                if (aligned[j]['span'][1] > particle_end or not aligned[j]['pos']
                        or aligned[j]['pos'][0] != '助詞'
                        or aligned[j - 1]['span'][1] != aligned[j]['span'][0]):
                    break
                j += 1
            if particle and (aligned[j - 1]['span'][1] != particle_end):
                continue
            if j >= len(aligned) or aligned[j - 1]['span'][1] != aligned[j]['span'][0]:
                continue
            verb = aligned[j]
            forms = {form for spelling in entry['verb'] for form in _verb_forms(spelling, entry['verb_class'])}
            expected_pos = '形容詞' if entry['verb_class'] == 'i_adjective' else '動詞'
            if not verb['pos'] or verb['pos'][0] != expected_pos or verb['lemma'] not in forms:
                continue
            end = j + 1
            if (expected_pos == '形容詞' and verb['surface'].endswith('く') and end < len(aligned)
                    and verb['span'][1] == aligned[end]['span'][0]
                    and aligned[end]['pos'][:1] == ['形容詞'] and aligned[end]['lemma'] == 'ない'):
                end += 1
            while (end < len(aligned) and aligned[end - 1]['span'][1] == aligned[end]['span'][0]
                   and aligned[end]['pos'] and aligned[end]['pos'][0] == '助動詞'):
                end += 1
            if (end < len(aligned) and aligned[end - 1]['span'][1] == aligned[end]['span'][0]
                    and aligned[end]['surface'] in {'て', 'で', 'ば'}
                    and aligned[end]['pos'][:2] == ['助詞', '接続助詞']):
                end += 1
            if expected_pos == '形容詞':
                prefix = sentence.text[first['span'][0]:verb['span'][0]]
                boundaries = {token['span'][1]: index + 1 for index, token in enumerate(aligned)}
                inflections = {form for spelling in entry['verb']
                               for form in _surface_forms(spelling, entry['verb_class'])}
                ends = [first['span'][0] + len(prefix) + len(form) for form in inflections
                        if sentence.text.startswith(prefix + form, first['span'][0])
                        and first['span'][0] + len(prefix) + len(form) in boundaries]
                if ends:
                    end = max(end, boundaries[max(ends)])
            parts = (component for token in aligned[i:end]
                     for component in token_components(raw[token['index']]))
            found.append(ExpressionMatch(entry, first['span'][0], aligned[end - 1]['span'][1],
                                         tuple(parts), omitted))
    return sorted(_recover_surface_matches(sentence, aligned, entries, found),
                  key=lambda match: (match.start, match.end, match.entry['expression']))


class ExpressionStep(PipelineStep):
    """Record expression evidence on raw tokens before grammar reservation."""
    def __init__(self, diagnostics_path=None):
        self._processing_step = ProcessingStep.WORD_COLLECTION
        self.start_time = None
        self.diagnostics_path = Path(diagnostics_path) if diagnostics_path is not None else None
        self.matches = []
        self.positions = {}

    def process(self, artifact):
        self.matches = []
        self.positions = {}
        source_totals = {}
        first_positions = {}
        for index, sentence in enumerate(artifact.data):
            matches = find_expressions(sentence)
            self.matches.append(matches)
            aligned, error = aligned_tokens(sentence.text, sentence.tokens)
            if error:
                continue
            origin = sentence.origin
            total = source_totals.get(origin, 0)
            eligible = [token for token in aligned if not is_useless(sentence.tokens[token['index']])]
            for match in matches:
                key = (origin, match.entry['expression'])
                before = sum(token['span'][0] < match.start for token in eligible)
                first_positions.setdefault(key, total + before + 1)
            source_totals[origin] = total + len(eligible)
            if (index + 1) % 2000 == 0 and index + 1 < len(artifact.data):
                self.progress(index + 1, len(artifact.data), 'Matching expressions')
        for (origin, word), position in first_positions.items():
            total = source_totals[origin]
            if total:
                self.positions.setdefault(word, {})[origin] = min(position, total) / total
        if self.diagnostics_path is not None:
            temporary = self.diagnostics_path.with_suffix(self.diagnostics_path.suffix + '.tmp')
            with temporary.open('w', encoding='utf-8', newline='\n') as stream:
                for index, (sentence, matches) in enumerate(zip(artifact.data, self.matches)):
                    for match in matches:
                        record = {'sentence_index': index, 'source': str(sentence.origin) if sentence.origin else None,
                                  'tag': sentence.tag, 'sentence': sentence.text,
                                  'candidate': match.entry['expression'],
                                  'surface': sentence.text[match.start:match.end],
                                  'span': [match.start, match.end],
                                  'omitted_particle': match.omitted_particle,
                                  'analysis_rule': match.analysis_rule,
                                  'components': [asdict(component) for component in match.components]}
                        stream.write(json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n')
            temporary.replace(self.diagnostics_path)
        self.done(f'{sum(map(len, self.matches))} expression occurrences found.')
        return artifact


class AddExpressionCandidatesStep(PipelineStep):
    """Add matches alongside lexical and grammar analyses, preserving evidence."""
    def __init__(self, expression_step):
        self.expression_step = expression_step
        self._processing_step = ProcessingStep.WORD_COLLECTION
        self.start_time = None

    def process(self, artifact):
        words = artifact.data
        inventory_words = {entry['expression'] for entry in inventory()}
        initial_words = inventory_words.intersection(words)
        existing_sources = {}
        for sentence in artifact.sentences:
            for word in sentence.lemma_occurrences.keys() & inventory_words & initial_words:
                existing_sources.setdefault(word, set()).add(sentence.origin)
        for sentence in artifact.sentences:
            index = sentence.source_index
            if index is None or not 0 <= index < len(self.expression_step.matches):
                continue
            for match in self.expression_step.matches[index]:
                entry = match.entry
                word = entry['expression']
                stats = words.get(word)
                occurrences = sentence.lemma_occurrences.get(word, ())
                # A lexical analysis may already cover this source occurrence.
                # Retain additional occurrences without counting overlaps twice.
                if any(span and span[0] >= 0 and span[0] < match.end and match.start < span[1]
                       for _, span, _ in occurrences):
                    continue
                if stats is None:
                    positions = tuple(self.expression_step.positions.get(word, {}).values())
                    stats = WordStats(sum(positions) / len(positions) if positions else 1.0,
                                      len(positions), 0, 0.0, entry['reading'], '', set(), [],
                                      word, ('exp',), invalid=False,
                                      expression_definition=entry['meaning'], expression_review=True)
                    words[word] = stats
                else:
                    stats.expression_definition = entry['meaning']
                    stats.expression_review = True
                    sources = existing_sources.setdefault(word, set())
                    position = self.expression_step.positions.get(word, {}).get(sentence.origin)
                    if word in initial_words and sentence.origin not in sources and position is not None:
                        stats.index = (stats.index * stats.index_count + position) / (stats.index_count + 1)
                        stats.index_count += 1
                        sources.add(sentence.origin)
                stats.frequency += 1
                stats.component_counts[match.components] = stats.component_counts.get(match.components, 0) + 1
                if sentence.tag:
                    stats.tags.add(sentence.tag)
                surface = sentence.text[match.start:match.end]
                span = (match.start, match.end)
                sentence.lemma_surfaces[word] = surface
                sentence.lemma_spans[word] = span
                sentence.lemma_components[word] = match.components
                sentence.lemma_occurrences.setdefault(word, []).append((surface, span, match.components))
        self.done('Added expression candidates alongside their component analyses.')
        return artifact
