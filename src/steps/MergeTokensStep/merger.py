"""Greedy dictionary-based token merging."""

from src.JMDict import JMDict
from src.TokenProvenance import token_components

from .helpers import (
    _has_useful_kana_only_sense,
    _is_prefixed_kana_noun,
    _is_split_kana_noun,
    _is_contracted_kana_verb,
    _is_noun_arige,
    has_kanji,
    is_dictionary_expression,
)
from .grammar_guards import (
    _crosses_incompatible_grammar,
    _is_past_predicate_misread_as_expression,
    _is_particle_boundary_misread,
    _is_inflection_misread_as_noun,
    _is_auxiliary_oru_misread,
    _is_kigakawatta_predicate,
    _is_prenominal_entry_in_past_predicate,
)
from .recovery import prepare_tokens


def _merge_candidate(token_list, i, j, combined_text, offsets, jmdict, rejected):
    """Return a dictionary merge for a candidate span, if eligible."""
    n = len(token_list)
    combined_surface = combined_text[offsets[i]:offsets[j]]
    # A merged card must be grounded in the text the learner will see.
    # Matching a concatenation of dictionary lemmas can invent an
    # unrelated word from inflected or grammatical tokens: for example,
    # おまち (polite stem of 待つ) used to become おまつ (black pine).
    # Surface matching still permits genuine compounds such as
    # おまもり, while leaving non-exact sequences as their original
    # tokens for high-recall mining.
    token_string = combined_surface
    # This attested mixed spelling uses kana for the final verb.
    # Keep the source surface while resolving the dictionary identity.
    if (combined_surface == '一刻をあらそう'
            and [t['surface'] for t in token_list[i:j]] == ['一刻', 'を', 'あらそう']
            and token_list[j - 1]['pos'][0] == '動詞'):
        token_string = '一刻を争う'
    definitions = jmdict.lookup_word(token_string)
    if not definitions:
        return None

    def reject(reason):
        if rejected is not None:
            rejected.append({"surface": token_string, "start_token": i, "end_token": j,
                             "components": [t["surface"] for t in token_list[i:j]], "reason": reason})

    match_word = None
    if (
        not has_kanji(token_string)
        and not _has_useful_kana_only_sense(definitions)
        and not _is_prefixed_kana_noun(token_list[i:j], definitions)
        and not _is_split_kana_noun(token_list[i:j], definitions)
        and not _is_noun_arige(token_list[i:j], definitions)
        and not (token_string == 'まけずぎらい'
                and [t['surface'] for t in token_list[i:j]] == ['まけ', 'ず', 'ぎらい'])
    ):
        if not any(_is_contracted_kana_verb(token_list[i:j], entry) for entry in definitions):
            reject("kana_entry_not_eligible_for_merge")
            return None
        match_word = jmdict.get_best_entry(token_string)
        if not _is_contracted_kana_verb(token_list[i:j], match_word):
            reject("kana_entry_not_eligible_for_merge")
            return None
    if match_word is None:
        match_word = jmdict.get_best_entry(token_string)
    if not match_word:
        reject("no_best_entry")
        return None
    if token_string == 'ちょっと見':
        following = token_list[j] if j < n else None
        if (following is not None
                and not (following['pos'][0] in {'補助記号', '記号'}
                         or (following['pos'][0] == '助詞'
                             and tuple(following['pos'][:2]) != ('助詞', '接続助詞'))
                         or (following['pos'][0] == '助動詞'
                             and following['lemma'] in {'だ', 'です'}))):
            reject('chotto_mi_requires_noun_context')
            return None
    # In 従っておく, 従って is the て-form of 従う, not the
    # dictionary adverb meaning "therefore". Preserve its verb evidence.
    if (token_string == '従って' and j < n
            and token_list[i]['lemma'] == '従う'
            and token_list[j]['lemma'] == 'おく'
            and tuple(token_list[j]['pos'][:2]) == ('動詞', '非自立可能')):
        reject('te_oku_verb_not_adverb')
        return None
    if (_is_past_predicate_misread_as_expression(token_list, i, j, match_word)
            or _is_particle_boundary_misread(token_list, i, j)
            or _is_inflection_misread_as_noun(token_list, i, j, match_word)
            or _is_auxiliary_oru_misread(token_list, i, j)
            or (any(t.get('analysis_rule') == 'object_tsureru_motion' for t in token_list[i:j])
                and not (token_list[i].get('analysis_rule') == 'object_tsureru_motion'
                         and j - i >= 3))):
        reject("inflection_or_motion_boundary")
        return None
    if _is_kigakawatta_predicate(token_list, i, j):
        reject("kigakawatta_predicate")
        return None
    if _is_prenominal_entry_in_past_predicate(token_list, i, j, match_word):
        reject("prenominal_in_past_predicate")
        return None
    if _crosses_incompatible_grammar(token_list[i:j], match_word):
        reject("incompatible_grammar")
        return None

    jmdict_pos = "exp"
    if hasattr(jmdict, "get_pos"):
        jmdict_pos = jmdict.get_pos(match_word)
    elif hasattr(match_word, "pos"):
        jmdict_pos = match_word.pos

    if isinstance(jmdict_pos, str):
        new_pos = [jmdict_pos, "*", "*", "*", "*", "*"]
    elif isinstance(jmdict_pos, list):
        new_pos = jmdict_pos + ["*"] * max(0, 6 - len(jmdict_pos))
    else:
        new_pos = list(token_list[j - 1]["pos"])

    lemma_str = match_word if isinstance(match_word, str) else token_string
    return {
        "surface": combined_surface,
        "lemma": lemma_str,
        "base_form": lemma_str,
        "reading": jmdict.get_reading(match_word, token_string),
        "analysis_rule": "jmdict_surface_merge",
        "dictionary_expression": is_dictionary_expression(match_word),
        "pos": new_pos,
        "components": tuple(
            component
            for token in token_list[i:j]
            for component in token_components(token)
        ),
    }


def _merge_pinto_kuru(parts, jmdict):
    """Recover the inflected expression, including dialectal こん."""
    verb = parts[2]
    if not (parts[0]['surface'] in ('ピン', 'ぴん') and parts[1]['surface'] == 'と'
            and ((verb['pos'][0] == '動詞' and verb['lemma'] in ('来る', 'くる'))
                 or verb['surface'] == 'こん')):
        return None
    word = parts[0]['surface'] + 'と来る'
    entries = jmdict.lookup_word(word)
    entry = jmdict.get_best_entry(word) if entries else None
    if not is_dictionary_expression(entry):
        return None
    return {
        'surface': ''.join(t['surface'] for t in parts),
        'lemma': word, 'base_form': word,
        'reading': jmdict.get_reading(entry, word),
        'pos': ['exp', '*', '*', '*', '*', '*'],
        'dictionary_expression': True,
        'analysis_rule': 'jmdict_pinto_kuru',
        'components': tuple(c for token in parts for c in token_components(token)),
    }


def _merge_adjective_stem(tokens, i, boundary, jmdict):
    """Recover a split dictionary adjective before appearance or -garu."""
    for end in range(min(boundary, i + 4), i + 1, -1):
        stem = tokens[end - 1]
        if end >= len(tokens) or stem['pos'][0] not in {'形容詞', '接尾辞'}:
            continue
        if not stem['lemma'].endswith('い') or stem['surface'] != stem['lemma'][:-1]:
            continue
        following = tokens[end]
        if not ((following['surface'] == 'そう' and following['pos'][:2] in
                 (('形状詞', '助動詞語幹'), ['形状詞', '助動詞語幹']))
                or (following['lemma'] == 'がる' and following['pos'][:2] in
                    (('接尾辞', '動詞的'), ['接尾辞', '動詞的']))):
            continue
        parts = tokens[i:end]
        if any(t['pos'][0] in {'助詞', '助動詞', '補助記号', '記号'} for t in parts):
            continue
        surface = ''.join(t['surface'] for t in parts)
        word = surface + 'い'
        entry = jmdict.get_best_entry(word) if jmdict.lookup_word(word) else None
        if not isinstance(entry, dict) or not any(
                tag.lower() == 'adjective (keiyoushi)'
                for sense in entry.get('senses', []) for tag in sense.get('pos', [])):
            continue
        forms = entry.get('kanji', []) or entry.get('reading', [])
        lemma = forms[0]['form'] if forms else word
        return {
            'surface': surface, 'lemma': lemma, 'base_form': lemma,
            'reading': jmdict.get_reading(entry, word),
            'pos': ['形容詞', '一般', '*', '*', '形容詞', '語幹-一般'],
            'analysis_rule': 'jmdict_adjective_stem',
            'components': tuple(c for token in parts for c in token_components(token)),
        }, end
    return None


def _merge_omitted_particle(token_list, i, next_reserved, jmdict):
    """Recover dictionary expressions with an omitted object particle."""
    if i + 1 < len(token_list) and next_reserved > i + 1:
        noun, verb = token_list[i:i + 2]
        if (noun['pos'][0] == '名詞' and verb['pos'][0] == '動詞'
                and noun['surface'] == noun['lemma']):
            word = noun['surface'] + 'を' + verb['lemma']
            entries = jmdict.lookup_word(word)
            entry = jmdict.get_best_entry(word) if entries else None
            if is_dictionary_expression(entry):
                return {
                    'surface': noun['surface'] + verb['surface'],
                    'lemma': word, 'base_form': word,
                    'reading': jmdict.get_reading(entry, word),
                    'pos': ['exp', '*', '*', '*', '*', '*'],
                    'dictionary_expression': True,
                    'analysis_rule': 'jmdict_omitted_object_particle',
                    'components': tuple(c for token in (noun, verb)
                                        for c in token_components(token)),
                }
    return None


def combine_tokens_jmdict(
    token_list: list[dict],
    jmdict: JMDict,
    max_lookahead: int = 10,
    rejected: list | None = None,
) -> list[dict]:
    """Greedily merge adjacent lexical compounds recognized by JMdict."""
    token_list = prepare_tokens(token_list, jmdict)
    merged_tokens = []
    n = len(token_list)
    surfaces = [token["surface"] for token in token_list]
    combined_text = "".join(surfaces)
    offsets = [0]
    for surface in surfaces:
        offsets.append(offsets[-1] + len(surface))
    # Exact dictionary merges may not cross reserved grammar or synthetic
    # boundary-repair tokens. The omitted-particle fallback only uses the
    # reserved boundary, so keep it separate.
    next_reserved = [n] * n
    next_merge_boundary = [n] * n
    reserved_boundary = merge_boundary = n
    for index in range(n - 1, -1, -1):
        token = token_list[index]
        if token.get('grammar_reserved'):
            reserved_boundary = merge_boundary = index
        elif token.get('analysis_rule') in ('desu_kitto_boundary', 'kara_sa_boundary'):
            merge_boundary = index
        next_reserved[index] = reserved_boundary
        next_merge_boundary[index] = merge_boundary
    i = 0

    while i < n:
        if token_list[i].get('grammar_reserved'):
            merged_tokens.append(token_list[i])
            i += 1
            continue
        if i + 3 <= min(n, i + max_lookahead, next_merge_boundary[i]):
            candidate = _merge_pinto_kuru(token_list[i:i + 3], jmdict)
            if candidate is not None:
                merged_tokens.append(candidate)
                i += 3
                continue
        stem_match = _merge_adjective_stem(token_list, i, next_reserved[i], jmdict)
        if stem_match is not None:
            candidate, i = stem_match
            merged_tokens.append(candidate)
            continue
        match_found = False
        for j in range(min(n, i + max_lookahead, next_merge_boundary[i]), i + 1, -1):
            candidate = _merge_candidate(token_list, i, j, combined_text, offsets, jmdict, rejected)
            if candidate is None:
                continue
            merged_tokens.append(candidate)
            i = j
            match_found = True
            break

        if not match_found:
            candidate = _merge_omitted_particle(token_list, i, next_reserved[i], jmdict)
            if candidate is not None:
                merged_tokens.append(candidate)
                i += 2
                continue

        if not match_found:
            merged_tokens.append(token_list[i])
            i += 1

    return merged_tokens
