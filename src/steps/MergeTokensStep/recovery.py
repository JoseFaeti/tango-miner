"""Recover source-supported token boundaries and analyses."""

from src.TokenProvenance import token_components

from .helpers import is_dictionary_expression


def _repair_moushi_boundaries(tokens):
    """Recover kana nouns split into an unrelated もうし expression."""
    result = []
    i = 0
    while i < len(tokens):
        parts = tokens[i:i + 3]
        surfaces = [part['surface'] for part in parts]
        lemmas = [part['lemma'] for part in parts]
        if (surfaces == ['しょう', 'もう', 'し'] and lemmas == ['する', 'もう', 'する']
                and i > 0 and tokens[i - 1]['surface'] == 'を'
                and i + 3 < len(tokens) and tokens[i + 3]['surface'] in {'て', 'た'}):
            noun_parts, lemma, reading = parts[:2], '消耗', 'しょうもう'
            length = 2
        elif (surfaces == ['もう', 'し', 'で'] and lemmas == ['もう', 'する', 'で']
              and i + 3 < len(tokens) and tokens[i + 3]['surface'] in {'が', 'を', 'に', 'は', 'も'}
              and tokens[i + 3]['pos'][0] == '助詞'):
            noun_parts, lemma, reading = parts, '申し出', 'もうしで'
            length = 3
        else:
            result.append(tokens[i])
            i += 1
            continue
        if any(part.get('grammar_reserved') for part in noun_parts):
            result.append(tokens[i])
            i += 1
            continue
        result.append({'surface': ''.join(part['surface'] for part in noun_parts),
                       'lemma': lemma, 'base_form': lemma, 'reading': reading,
                       'pos': ('名詞', '普通名詞', 'サ変可能', '*', '*', '*'),
                       'components': tuple(c for part in noun_parts for c in token_components(part)),
                       'analysis_rule': 'moushi_noun_boundary'})
        i += length
    return result


def _repair_tsurete_motion(tokens, index, jmdict):
    """Identify taking someone along only with an object and motion verb."""
    token = tokens[index]
    if (token['lemma'] != 'つれる' or token['pos'][0] != '動詞'
            or index + 2 >= len(tokens) or tokens[index + 1]['surface'] != 'て'
            or tokens[index + 1]['pos'][0] != '助詞'
            or tokens[index + 2]['lemma'] not in {'いく', '行く', 'くる', '来る'}
            or tokens[index + 2]['pos'][0] != '動詞'):
        return token
    prior = tokens[max(0, index - 6):index]
    object_positions = [i for i, t in enumerate(prior)
                        if t['surface'] == 'を' and t['pos'][0] == '助詞']
    if not object_positions or any(t['surface'] in {'。', '！', '？', '!', '?', '…'}
                                   for t in prior[object_positions[-1] + 1:]):
        return token
    entries = jmdict.lookup_word('連れる')
    if not any(any(r['form'] == 'つれる' for r in e.get('reading', []))
               and any(tag.lower().startswith('ichidan verb') for sense in e.get('senses', [])
                       for tag in sense.get('pos', [])) for e in entries):
        return token
    return {**token, 'lemma': '連れる', 'base_form': '連れる', 'reading': 'つれる',
            'components': token_components(token), 'analysis_rule': 'object_tsureru_motion'}

def _repair_karai_boundaries(tokens: list[dict]) -> list[dict]:
    """Recover source-supported colloquial boundaries misread as からい."""
    result = []
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if (i + 1 < len(tokens) and token['surface'] == 'ズラ'
                and tokens[i + 1]['surface'] == 'かろう'
                and tokens[i + 1]['lemma'] == 'からい'):
            evidence = tuple(c for t in tokens[i:i + 2] for c in token_components(t))
            result.append({'surface': 'ズラかろう', 'lemma': 'ズラかる', 'base_form': 'ズラかる',
                           'reading': 'ずらかる', 'pos': ('動詞', '一般', '*', '*', '五段-ラ行', '意志推量形'),
                           'components': evidence, 'analysis_rule': 'zurakaru_boundary'})
            i += 2
        elif (i > 0 and i + 1 < len(tokens) and token['surface'] == 'から'
              and token['lemma'] == 'からい' and tokens[i + 1]['surface'] == 'さ'
              and tokens[i + 1]['pos'][0] == '接尾辞'
              and (tokens[i - 1]['surface'] == 'くっ'
                   or (i + 2 < len(tokens) and tokens[i - 1]['surface'] == 'カルバナイル'
                       and tokens[i + 2]['surface'] == 'エリカ'))):
            result.extend([
                {'surface': 'から', 'lemma': 'から', 'base_form': 'から', 'reading': 'から',
                 'pos': ('助詞', '接続助詞', '*', '*', '*', '*'),
                 'components': token_components(token), 'analysis_rule': 'kara_sa_boundary'},
                {'surface': 'さ', 'lemma': 'さ', 'base_form': 'さ', 'reading': 'さ',
                 'pos': ('助詞', '終助詞', '*', '*', '*', '*'),
                 'components': token_components(tokens[i + 1]), 'analysis_rule': 'kara_sa_boundary'},
            ])
            i += 2
        else:
            result.append(token)
            i += 1
    return result

def _repair_komatta_koto_boundary(tokens: list[dict]) -> list[dict]:
    """Expose the problem noun inside Sudachi's merged ことがある token."""
    result = []
    for i, token in enumerate(tokens):
        if (i >= 2 and i + 1 < len(tokens)
                and tokens[i - 2]['surface'] == '困っ' and tokens[i - 2]['lemma'] == '困る'
                and tokens[i - 1]['surface'] == 'た' and tokens[i - 1]['pos'][:1] in (('助動詞',), ['助動詞'])
                and token['surface'] == token['lemma'] == 'ことがある'
                and tokens[i + 1]['surface'] == 'なら' and tokens[i + 1]['lemma'] == 'だ'
                and not (i >= 3 and tokens[i - 3]['surface'] == 'に'
                         and tokens[i - 3]['pos'][:1] in (('助詞',), ['助詞']))):
            evidence = token_components(token)
            result.extend([
                {'surface': 'こと', 'lemma': 'こと', 'base_form': 'こと', 'reading': 'こと',
                 'pos': ('名詞', '普通名詞', '一般', '*', '*', '*'),
                 'components': evidence, 'analysis_rule': 'komatta_koto_boundary'},
                {'surface': 'が', 'lemma': 'が', 'base_form': 'が', 'reading': 'が',
                 'pos': ('助詞', '格助詞', '*', '*', '*', '*'),
                 'components': evidence, 'analysis_rule': 'komatta_koto_boundary'},
                {'surface': 'ある', 'lemma': 'ある', 'base_form': 'ある', 'reading': 'ある',
                 'pos': ('動詞', '非自立可能', '*', '*', '五段-ラ行', '終止形-一般'),
                 'components': evidence, 'analysis_rule': 'komatta_koto_boundary'},
            ])
        else:
            result.append(token)
    return result

def _repair_chigee_boundary(tokens: list[dict]) -> list[dict]:
    """Recover colloquial ちげぇ after それは from a split numeral analysis."""
    result = []
    i = 0
    while i < len(tokens):
        if (i > 0 and i + 2 < len(tokens) and tokens[i - 1]['surface'] == 'それ'
                and [t['surface'] for t in tokens[i:i + 3]] == ['はち', 'げ', 'ぇ']
                and tokens[i]['pos'][:2] in (('名詞', '数詞'), ['名詞', '数詞'])
                and tokens[i + 1]['pos'][:1] in (('接尾辞',), ['接尾辞'])
                and tokens[i + 2]['pos'][:1] in (('補助記号',), ['補助記号'])):
            evidence = tuple(c for t in tokens[i:i + 3] for c in token_components(t))
            result.extend([
                {'surface': 'は', 'lemma': 'は', 'base_form': 'は', 'reading': 'は',
                 'pos': ('助詞', '係助詞', '*', '*', '*', '*'),
                 'components': token_components(tokens[i]), 'analysis_rule': 'chigee_boundary'},
                {'surface': 'ちげぇ', 'lemma': 'ちげぇ', 'base_form': 'ちげぇ', 'reading': 'ちげぇ',
                 'pos': ('形容詞', '一般', '*', '*', '*', '*'),
                 'components': evidence, 'analysis_rule': 'chigee_boundary'},
            ])
            i += 3
        else:
            result.append(tokens[i])
            i += 1
    return result

def _repair_sukitto_and_rise(tokens: list[dict]) -> list[dict]:
    """Recover source-supported boundaries without discarding analyzer evidence."""
    result = []
    for index, token in enumerate(tokens):
        previous = tokens[index - 1] if index else None
        following = tokens[index + 1] if index + 1 < len(tokens) else None
        if (token['surface'] == token['lemma'] == 'すきっと' and previous
                and previous['surface'] == 'で'
                and ((previous['lemma'] == 'だ' and previous['pos'][0] == '助動詞')
                     or (index >= 2 and tokens[index - 2]['surface'] == 'くらい'
                         and previous['pos'][0] == '助詞')) and following
                and (following['pos'][0] == '補助記号'
                     or (following['pos'][0] == '名詞' and len(following['pos']) > 2
                         and 'サ変' in following['pos'][2]))):
            evidence = token_components(token)
            result.extend([
                {'surface': 'す', 'lemma': 'です', 'base_form': 'です', 'reading': 'です',
                 'pos': ('助動詞', '*', '*', '*', '*', '*'), 'components': evidence,
                 'analysis_rule': 'desu_kitto_boundary'},
                {'surface': 'きっと', 'lemma': 'きっと', 'base_form': 'きっと', 'reading': 'きっと',
                 'pos': ('副詞', '*', '*', '*', '*', '*'), 'components': evidence,
                 'analysis_rule': 'desu_kitto_boundary'},
            ])
        elif (token['surface'] == 'りせ' and token['lemma'] == 'りする' and following
              and (following['surface'] in {'の', 'は', 'が', 'を', 'に', 'と', 'も', 'から', 'だけ'}
                   and following['pos'][0] == '助詞'
                   or following['surface'] == 'ちゃん' and following['pos'][0] == '接尾辞'
                   or following['surface'] == 'だ' and following['pos'][0] == '助動詞')):
            result.append({**token, 'lemma': 'りせ', 'base_form': 'りせ', 'reading': 'りせ',
                           'pos': ('名詞', '固有名詞', '人名', '*', '*', '*'),
                           'components': token_components(token), 'analysis_rule': 'rise_name_boundary'})
        else:
            result.append(token)
    return result

def _repair_warui_koto_boundary(tokens: list[dict]) -> list[dict]:
    """Recover the adjective and noun before continuative する."""
    result = []
    for index, token in enumerate(tokens):
        following = tokens[index + 1] if index + 1 < len(tokens) else None
        if (token['surface'] == token['lemma'] == '悪いこと'
                and following and following['surface'] == 'し'
                and following['lemma'] == 'する'
                and following['pos'][:1] in (('動詞',), ['動詞'])):
            evidence = token_components(token)
            result.extend([
                {'surface': '悪い', 'lemma': '悪い', 'base_form': '悪い', 'reading': 'わるい',
                 'pos': ('形容詞', '一般', '*', '*', '形容詞', '連体形-一般'),
                 'components': evidence, 'analysis_rule': 'warui_koto_boundary'},
                {'surface': 'こと', 'lemma': 'こと', 'base_form': 'こと', 'reading': 'こと',
                 'pos': ('名詞', '普通名詞', '一般', '*', '*', '*'),
                 'components': evidence, 'analysis_rule': 'warui_koto_boundary'},
            ])
        else:
            result.append(token)
    return result

def _repair_kotoshi_boundaries(token_list: list[dict]) -> list[dict]:
    """Split ことし when the following token confirms する inflection."""
    result = []
    for index, token in enumerate(token_list):
        following = token_list[index + 1] if index + 1 < len(token_list) else None
        if (token["surface"] == "ことし" and token["lemma"] == "ことし"
                and token["pos"][0] == "名詞" and following
                and following["surface"] in {"て", "ても", "ない", "なくても", "なくちゃ", "なければ", "ちゃあ"}):
            evidence = token_components(token)
            result.extend([
                {"surface": "こと", "lemma": "こと", "base_form": "こと", "reading": "こと",
                 "pos": ("名詞", "普通名詞", "一般", "*", "*", "*"), "components": evidence, "analysis_rule": "kotoshi_boundary"},
                {"surface": "し", "lemma": "する", "base_form": "する", "reading": "する",
                 "pos": ("動詞", "非自立可能", "*", "*", "サ行変格", "連用形-一般"), "components": evidence, "analysis_rule": "kotoshi_boundary"},
            ])
        else:
            result.append(token)
    return result

def _repair_kigahen_boundaries(token_list: list[dict], jmdict) -> list[dict]:
    """Recover 変わる when the preceding token has swallowed its first kanji."""
    result = []
    i = 0
    while i < len(token_list):
        parts = token_list[i:i + 3]
        if (len(parts) == 3 and not any(t.get('grammar_reserved') for t in parts[:2])
                and parts[0]['surface'] == '気が変'
                and parts[0]['lemma'] == '気が変' and parts[0]['pos'][0] == '形状詞'
                and parts[1]['surface'] == 'わっ' and parts[1]['lemma'] == 'わる'
                and parts[1]['pos'][0] == '動詞'
                and ((parts[2]['lemma'] == 'た' and parts[2]['pos'][0] == '助動詞')
                     or (parts[2]['surface'] == 'て' and parts[2]['pos'][0] == '助詞'))
                and jmdict.lookup_word('変わる')):
            from src.steps.TokenizeStep.index import get_tokenizer, TOKENIZER_MODE
            nodes = get_tokenizer().tokenize('変わっ' + parts[2]['surface'], TOKENIZER_MODE)
            if (nodes and nodes[0].surface() == '変わっ'
                    and nodes[0].dictionary_form() == '変わる'
                    and nodes[0].part_of_speech()[0] == '動詞'):
                first, second = parts[:2]
                evidence = token_components(first)
                result.extend([
                    {'surface': '気', 'lemma': '気', 'base_form': '気', 'reading': 'き',
                     'pos': ('名詞', '普通名詞', '一般', '*', '*', '*'), 'components': evidence, 'analysis_rule': 'kigahen_boundary'},
                    {'surface': 'が', 'lemma': 'が', 'base_form': 'が', 'reading': 'が',
                     'pos': ('助詞', '格助詞', '*', '*', '*', '*'), 'components': evidence, 'analysis_rule': 'kigahen_boundary'},
                    {'surface': '変わっ', 'lemma': '変わる', 'base_form': '変わる', 'reading': 'かわる',
                     'pos': tuple(nodes[0].part_of_speech()),
                     'components': evidence + token_components(second), 'reassigned_from': 'わる', 'analysis_rule': 'kigahen_boundary'},
                ])
                i += 2
                continue
        result.append(token_list[i])
        i += 1
    return result

def _match_kamoshirenai(parts: list[dict]) -> int:
    """Return the length of a verified negative construction, or zero."""
    if (len(parts) < 4 or [t['surface'] for t in parts[:3]] != ['か', 'も', 'しれ']
            or [t['pos'][0] for t in parts[:3]] != ['助詞', '助詞', '動詞']
            or parts[2]['lemma'] != 'しれる'):
        return 0
    negative_index = 3
    if (parts[3]['surface'] == 'ませ' and parts[3]['lemma'] == 'ます'
            and len(parts[3]['pos']) > 4 and parts[3]['pos'][4] == '助動詞-マス'):
        negative_index = 4
    if len(parts) <= negative_index:
        return 0
    negative = parts[negative_index]
    if (len(negative['pos']) > 4 and negative['pos'][0] == '助動詞'
            and ((negative['surface'] in {'ねえ', 'ねぇ'} and negative['pos'][4] == '助動詞-ナイ' and negative_index == 3)
                 or (negative['surface'] in ({'ぬ'} if negative_index == 4 else {'ぬ', 'ん'})
                     and negative['pos'][4] == '助動詞-ヌ'))):
        return negative_index + 1
    return 0

def _repair_grammatical_constructions(token_list: list[dict]) -> list[dict]:
    """Recognize evidenced constructions before greedy dictionary merging."""
    result = []
    i = 0
    while i < len(token_list):
        length = _match_kamoshirenai(token_list[i:i + 5])
        if (not length and token_list[i]['surface'] == 'かもしれん'
                and token_list[i]['lemma'] == 'かもしれん'):
            length = 1
        if length:
            parts = token_list[i:i + length]
            result.append({
                'surface': ''.join(t['surface'] for t in parts),
                'lemma': 'かもしれない', 'base_form': 'かもしれない',
                'reading': 'かもしれない', 'pos': ['exp', '*', '*', '*', '*', '*'],
                'analysis_rule': 'kamoshirenai_construction',
                'components': tuple(c for t in parts for c in token_components(t)),
            })
            i += length
        else:
            result.append(token_list[i])
            i += 1
    return result

def _repair_imperative(token_list: list[dict], index: int, jmdict) -> dict:
    """Repair a potential-lemma analysis of a clearly delimited imperative."""
    token = token_list[index]
    lemma = token["lemma"]
    if (index + 1 >= len(token_list) or token["pos"][0] != "動詞"
            or not lemma.endswith("る") or token["surface"] != lemma[:-1]
            or token_list[index + 1]["surface"] not in {"。", "！", "!", "？", "?", "、", "…", "って"}
            or jmdict.lookup_word(lemma)):
        return token
    from src.steps.TokenizeStep.index import get_tokenizer, TOKENIZER_MODE, kata_to_hira
    nodes = get_tokenizer().tokenize(lemma, TOKENIZER_MODE)
    if len(nodes) != 1 or nodes[0].dictionary_form() != lemma:
        return token
    base = nodes[0].normalized_form()
    endings = {"う": "える", "く": "ける", "ぐ": "げる", "す": "せる", "つ": "てる",
               "ぬ": "ねる", "ぶ": "べる", "む": "める", "る": "れる"}
    if not base or base[-1] not in endings or base[:-1] + endings[base[-1]] != lemma:
        return token
    # Restrict to godan verbs: the imperative of an ichidan verb differs.
    base_nodes = get_tokenizer().tokenize(base, TOKENIZER_MODE)
    if len(base_nodes) != 1 or "五段" not in base_nodes[0].part_of_speech()[4]:
        return token
    reading = kata_to_hira(base_nodes[0].reading_form())
    entries = jmdict.lookup_word(base)
    if not any(isinstance(entry, dict) and any(r["form"] == reading for r in entry.get("reading", []))
               and any("verb" in tag.lower() and not tag.lower().startswith(("adverb", "noun", "auxiliary"))
                       for sense in entry.get("senses", []) for tag in sense.get("pos", []))
               for entry in entries):
        return token
    return {**token, "lemma": base, "base_form": base, "reading": reading,
            "reassigned_from": lemma, "components": token_components(token), "analysis_rule": "imperative_reassignment"}

def _recover_me_ni_mieru(tokens, jmdict):
    """Keep the source-grounded expression before a reserved て-form ending."""
    result = []
    i = 0
    while i < len(tokens):
        parts = tokens[i:i + 4]
        if (len(parts) == 4 and [t['surface'] for t in parts[:3]] == ['目', 'に', '見え']
                and parts[0]['lemma'] == '目' and parts[1]['pos'][0] == '助詞'
                and parts[2]['lemma'] == '見える' and parts[2]['pos'][0] == '動詞'
                and parts[3].get('grammar_reserved')
                and parts[3]['surface'] in ('て', 'てる', 'てた', 'てない')):
            word = '目に見える'
            entries = jmdict.lookup_word(word)
            entry = jmdict.get_best_entry(word) if entries else None
            if is_dictionary_expression(entry):
                result.append({'surface': '目に見え', 'lemma': word, 'base_form': word,
                               'reading': jmdict.get_reading(entry, word),
                               'pos': ['exp', '*', '*', '*', '*', '*'],
                               'dictionary_expression': True,
                               'analysis_rule': 'inflected_expression_me_ni_mieru',
                               'components': tuple(c for t in parts[:3] for c in token_components(t))})
                i += 3
                continue
        result.append(tokens[i])
        i += 1
    return result


def _recover_flower_offering(tokens, jmdict):
    """Resolve the kana verb when its direct object is explicitly flowers."""
    if not jmdict.lookup_word('供える'):
        return tokens
    result = list(tokens)
    for i, token in enumerate(tokens):
        if (i < 2 or token.get('grammar_reserved') or token['lemma'] != 'そなえる'
                or token['pos'][0] != '動詞' or tokens[i - 1]['surface'] != 'を'
                or tokens[i - 1].get('grammar_reserved')
                or tokens[i - 1]['pos'][0] != '助詞'):
            continue
        flower = tokens[i - 2]
        if (flower.get('grammar_reserved') or flower['surface'] not in {'花', 'はな', 'お花', 'おはな'}
                or flower['pos'][0] not in {'名詞', 'exp'}):
            continue
        result[i] = {**token, 'lemma': '供える', 'base_form': '供える',
                     'components': token_components(token), 'analysis_rule': 'flower_offering'}
    return result


def prepare_tokens(token_list, jmdict):
    """Apply boundary and analysis repairs while preserving reserved tokens."""
    surfaces = {token['surface'] for token in token_list}
    if 'もう' in surfaces:
        token_list = _repair_moushi_boundaries(token_list)
    if '目' in surfaces:
        token_list = _recover_me_ni_mieru(token_list, jmdict)
    # The reserved ending remains context for this repair, but is never changed.
    if '気が変' in surfaces:
        token_list = _repair_kigahen_boundaries(token_list, jmdict)
    # Boundary repairs operate on lexical runs, while the merge guards below
    # can still inspect adjacent reserved tokens as context. Refresh the
    # surfaces after a repair so later repairs can see any new tokens.
    def repair_run(run):
        present = {token['surface'] for token in run}
        for triggers, repair in (
            (('ことがある',), _repair_komatta_koto_boundary),
            (('はち',), _repair_chigee_boundary),
            (('悪いこと',), _repair_warui_koto_boundary),
            (('ことし',), _repair_kotoshi_boundaries),
            (('か', 'かもしれん'), _repair_grammatical_constructions),
            (('すきっと', 'りせ'), _repair_sukitto_and_rise),
            (('ズラ', 'から'), _repair_karai_boundaries),
        ):
            if any(trigger in present for trigger in triggers):
                run = repair(run)
                present = {token['surface'] for token in run}
        return run

    if any(token.get('grammar_reserved') for token in token_list):
        repaired, start = [], 0
        for index, token in enumerate(token_list):
            if token.get('grammar_reserved'):
                repaired.extend(repair_run(token_list[start:index]))
                repaired.append(token)
                start = index + 1
        repaired.extend(repair_run(token_list[start:]))
        token_list = repaired
    else:
        token_list = repair_run(token_list)
    token_list = [_repair_imperative(token_list, i, jmdict) if not token.get('grammar_reserved') else token
                  for i, token in enumerate(token_list)]
    token_list = [_repair_tsurete_motion(token_list, i, jmdict) if not token.get('grammar_reserved') else token
                  for i, token in enumerate(token_list)]
    if 'を' in surfaces:
        token_list = _recover_flower_offering(token_list, jmdict)
    return token_list
