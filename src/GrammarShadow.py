"""Source-aligned, explicitly tentative grammar proposals from raw tokens."""
import hashlib
import json
from pathlib import Path

SCHEMA_VERSION = 2
RULE_VERSION = 'grammar-shadow-4'
CATALOG = {
    'explanatory_nda': {'display': 'んだ', 'reference': 'https://raw.githubusercontent.com/evanclan/OpenJLPT/main/data/json/grammar/n4.json#n-desu-no-desu'},
    'te_iru': {'display': '〜ている', 'reference': 'https://raw.githubusercontent.com/evanclan/OpenJLPT/main/data/json/grammar/n5.json#te-iru'},
    'concessive_monono': {'display': 'ものの', 'reference': 'https://raw.githubusercontent.com/evanclan/OpenJLPT/main/data/json/grammar/n2.json#mono-no'},
}


def fingerprint():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def aligned_tokens(text, tokens):
    """Return offsets or an error; never infer offsets across missing text."""
    result, cursor = [], 0
    for index, token in enumerate(tokens):
        surface = token['surface']
        if not surface:
            continue
        start = text.find(surface, cursor)
        if start < 0 or text[cursor:start].strip():
            return None, f'cannot align token {index}'
        end = start + len(surface)
        result.append({'index': index, 'span': [start, end], 'surface': surface,
                       'lemma': token.get('lemma'), 'pos': list(token.get('pos', ()))})
        cursor = end
    if text[cursor:].strip():
        return None, 'token stream omits source text'
    return result, None


def token_evidence(construction, start, end, tokens):
    """Describe observable morphology without claiming semantic certainty."""
    overlapping = [t for t in tokens if t['span'][0] < end and t['span'][1] > start]
    covers = bool(overlapping and overlapping[0]['span'][0] == start
                  and overlapping[-1]['span'][1] == end)
    surfaces = [t['surface'] for t in overlapping]
    evidence = ['token_boundaries_match' if covers else 'inside_token_boundary']
    if construction == 'explanatory_nda' and surfaces == ['ん', 'だ']:
        if overlapping[0]['pos'][:2] == ['助詞', '準体助詞'] and overlapping[1]['pos'][:1] == ['助動詞']:
            evidence.append('nominalizer_plus_copula_analysis')
    if construction == 'te_iru' and covers:
        if (len(overlapping) == 2 and surfaces[0] in ('て', 'で')
                and overlapping[0]['pos'][:2] == ['助詞', '接続助詞']
                and overlapping[1]['lemma'] in ('いる', '居る')):
            evidence.append('connective_plus_iru_analysis')
        elif len(overlapping) == 1 and overlapping[0]['lemma'] in ('てる', 'でる'):
            evidence.append('contracted_auxiliary_analysis')
    if construction == 'concessive_monono':
        if len(overlapping) == 1 and covers and overlapping[0]['pos'][:1] == ['接続詞']:
            evidence.append('conjunction_analysis_not_disambiguating')
        elif (surfaces == ['もの', 'の'] and covers
              and overlapping[0]['pos'][:1] == ['名詞']
              and overlapping[1]['pos'][:1] == ['助詞']):
            evidence.append('noun_plus_particle_analysis_not_disambiguating')
    return overlapping, evidence


def polite_suffix_end(tokens, index):
    """Extend documented split polite suffixes only when adjacent and supported."""
    token = tokens[index]
    end = index
    if token['lemma'] != 'ます' or token['pos'][:1] != ['助動詞']:
        return end

    def next_matches(surface, lemma, pos):
        next_index = end + 1
        return (next_index < len(tokens) and tokens[end]['span'][1] == tokens[next_index]['span'][0]
                and tokens[next_index]['surface'] == surface
                and tokens[next_index]['lemma'] == lemma
                and tokens[next_index]['pos'][:1] == [pos])

    if token['surface'] == 'まし':
        if next_matches('た', 'た', '助動詞') or next_matches('て', 'て', '助詞'):
            end += 1
    elif token['surface'] == 'ませ' and next_matches('ん', 'ぬ', '助動詞'):
        end += 1
        if (next_matches('でし', 'です', '助動詞') and end + 2 < len(tokens)
                and tokens[end + 1]['span'][1] == tokens[end + 2]['span'][0]
                and tokens[end + 2]['surface'] == 'た'
                and tokens[end + 2]['lemma'] == 'た'
                and tokens[end + 2]['pos'][:1] == ['助動詞']):
            end += 2
    return end


def inflected_te_iru_spans(tokens):
    """Propose observed inflected forms, preserving original token boundaries."""
    for index, token in enumerate(tokens):
        following = tokens[index + 1:index + 3]
        if (token['surface'] in ('て', 'で')
                and token['pos'][:2] == ['助詞', '接続助詞']
                and len(following) == 2 and token['span'][1] == following[0]['span'][0]
                and following[0]['span'][1] == following[1]['span'][0]
                and following[0]['lemma'] in ('いる', '居る')
                and following[0]['surface'] == 'い'
                and following[1]['lemma'] in ('た', 'ます')):
            end = polite_suffix_end(tokens, index + 2)
            yield token['span'][0], tokens[end]['span'][1], 'inflected_iru_plus_auxiliary_analysis'
        if (token['surface'] in ('て', 'で') and token['lemma'] in ('てる', 'でる')
                and token['pos'][:1] == ['助動詞'] and following
                and token['span'][1] == following[0]['span'][0]
                and following[0]['lemma'] in ('た', 'ます')):
            end = polite_suffix_end(tokens, index + 1)
            yield token['span'][0], tokens[end]['span'][1], 'contracted_inflected_auxiliary_analysis'


def analyze_sentence(sentence, index, source_id=None):
    text = sentence.text
    identity = hashlib.sha256(json.dumps([source_id if source_id is not None else [str(sentence.origin), index], text],
                                        ensure_ascii=False).encode('utf-8')).hexdigest()
    tokens, error = aligned_tokens(text, sentence.tokens)
    row = {'schema_version': SCHEMA_VERSION, 'rule_version': RULE_VERSION,
           'sentence_id': identity, 'sentence_index': index,
           'source': str(sentence.origin) if sentence.origin is not None else None,
           'tag': sentence.tag, 'normalized_sentence': text,
           'raw_sentence': None, 'raw_mapping_status': 'unavailable',
           'alignment_error': error, 'tokens': tokens or [], 'proposals': []}
    if error:
        return row
    for construction, forms in (('explanatory_nda', ('んだ',)),
                                ('te_iru', ('ている', 'でいる', 'てる', 'でる')),
                                ('concessive_monono', ('ものの',))):
        for form in forms:
            cursor = 0
            while (start := text.find(form, cursor)) >= 0:
                end = start + len(form)
                overlapping, evidence = token_evidence(construction, start, end, tokens)
                proposal_id = hashlib.sha256(f'{identity}\0{construction}\0{start}\0{end}'.encode()).hexdigest()
                row['proposals'].append({
                    'id': proposal_id, 'construction_id': construction,
                    'span': [start, end], 'surface': form, 'raw_span': None,
                    'status': 'ambiguous', 'reason': 'surface_match_requires_context',
                    'evidence': evidence,
                    'component_token_ids': [f'{identity}:{t["index"]}' for t in overlapping],
                    'reference': CATALOG[construction]['reference'],
                })
                cursor = start + 1
    for start, end, cue in inflected_te_iru_spans(tokens):
        overlapping, evidence = token_evidence('te_iru', start, end, tokens)
        proposal_id = hashlib.sha256(f'{identity}\0te_iru\0{start}\0{end}'.encode()).hexdigest()
        row['proposals'].append({
            'id': proposal_id, 'construction_id': 'te_iru', 'span': [start, end],
            'surface': text[start:end], 'raw_span': None, 'status': 'ambiguous',
            'reason': 'inflected_form_requires_context', 'evidence': evidence + [cue],
            'component_token_ids': [f'{identity}:{t["index"]}' for t in overlapping],
            'reference': CATALOG['te_iru']['reference'],
        })
    row['proposals'].sort(key=lambda p: (p['span'], p['construction_id']))
    for token in row['tokens']:
        token['id'] = f'{identity}:{token["index"]}'
    return row
