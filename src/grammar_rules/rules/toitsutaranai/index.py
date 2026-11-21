"""Express an extreme degree with といったらない or ったらない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toitsutaranai(tokens):
    for i in range(1, len(tokens)):
        before = tokens[i - 1]
        if before['pos'][:1] not in (['形容詞'], ['名詞'], ['代名詞'], ['動詞']):
            continue
        if tokens[i]['surface'] == 'ったら' and tokens[i]['pos'][:1] == ['助詞']:
            negative = i + 1
        elif (i + 2 < len(tokens) and tokens[i]['surface'] == 'と'
              and tokens[i]['pos'][:1] == ['助詞']
              and tokens[i + 1]['surface'] in ('いっ', '言っ')
              and tokens[i + 1]['lemma'] in ('いう', '言う')
              and tokens[i + 2]['surface'] == 'たら'):
            negative = i + 3
        else:
            continue
        if negative >= len(tokens):
            continue
        part = tokens[negative]
        if part['lemma'] != 'ない' or part['surface'] not in ('ない', 'なかっ'):
            continue
        end = negative
        if part['surface'] == 'なかっ':
            end += 1
            if end >= len(tokens) or tokens[end]['surface'] != 'た' or tokens[end]['lemma'] != 'た':
                continue
        if all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 1, end)):
            yield tokens[i]['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='toitsutaranai', expression='〜といったらない', reading='といったらない',
                   meaning='indescribably; extremely', match=match_toitsutaranai, order=30)
RULES = (RULE,)
