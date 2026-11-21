"""Temporal milestone expressed with にして."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


NISHITE_EXPRESSION = '〜にして'
_TIME_WORDS = {'時', '歳', '才', '年', '日', '月', '週', '時間', '分', '秒',
               '瞬間', '一瞬', '初めて', 'はじめて', '頃', 'ころ'}


def match_nishite(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ni, shi, te = tokens[i - 1:i + 3]
        if (previous['surface'] not in _TIME_WORDS
                or previous['pos'][:1] not in (['名詞'], ['接尾辞'], ['副詞'])
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 2], tokens[i:i + 3]))
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or shi['surface'] != 'し' or shi['lemma'] != 'する'
                or shi['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']):
            continue
        if (i + 3 < len(tokens) and adjacent(te, tokens[i + 3])
                and tokens[i + 3]['surface'] in ('は', 'も')):
            continue
        yield ni['span'][0], te['span'][1]


RULE = GrammarRule(id='nishite', expression=NISHITE_EXPRESSION, reading='にして',
                   meaning='at; only after (a temporal milestone)',
                   match=match_nishite, order=19)
RULES = (RULE,)
