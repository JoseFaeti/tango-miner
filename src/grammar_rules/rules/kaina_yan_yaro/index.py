"""Selected Kansai-style sentence endings."""
from src.GrammarConstants import KAINA_YAN_YARO_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kaina_yan_yaro(tokens):
    for i in range(1, len(tokens)):
        prev, marker = tokens[i - 1:i + 1]
        if not adjacent(prev, marker):
            continue
        pos = prev['pos']
        if pos[:1] in (['動詞'], ['形容詞'], ['助動詞']):
            if len(pos) < 6 or not pos[5].startswith(('終止形', '連体形')):
                continue
        elif pos[:1] not in (['名詞'], ['代名詞'], ['形状詞'], ['副詞']):
            continue
        if pos[:2] == ['名詞', '固有名詞']:
            continue
        end = i
        if (marker['surface'], marker['lemma'], marker['pos'][:2]) == ('か', 'か', ['助詞', '終助詞']):
            if i + 2 >= len(tokens):
                continue
            middle, last = tokens[i + 1:i + 3]
            if (not adjacent(marker, middle) or not adjacent(middle, last)
                    or (middle['surface'], middle['lemma'], middle['pos'][:2]) != ('い', 'い', ['助詞', '終助詞'])
                    or (last['surface'], last['lemma'], last['pos'][:2]) != ('な', 'な', ['助詞', '終助詞'])):
                continue
            end = i + 2
        elif marker['surface'] == 'やん' and marker['lemma'] == 'やん' and marker['pos'][:2] == ['接尾辞', '名詞的']:
            pass
        elif marker['surface'] == 'や' and marker['pos'][:1] in (['助動詞'], ['助詞']):
            if i + 1 >= len(tokens) or not adjacent(marker, tokens[i + 1]):
                continue
            last = tokens[i + 1]
            if (last['surface'], last['lemma'], last['pos'][:2]) != ('ん', 'ん', ['助詞', '終助詞']):
                continue
            end = i + 1
        elif (marker['surface'], marker['lemma'], marker['pos'][:1]) == ('やろ', 'や', ['助動詞']):
            pass
        else:
            continue
        yield marker['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='kaina_yan_yaro', expression=KAINA_YAN_YARO_EXPRESSION,
                   reading='かいな / やん / やろ',
                   meaning='dialectal wondering, shared recognition, or conjecture',
                   match=match_kaina_yan_yaro)
