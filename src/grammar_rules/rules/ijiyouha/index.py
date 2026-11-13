"""Consequential 以上 following an attributive predicate."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


IJIYOUHA_EXPRESSION = '〜以上は'


def match_ijiyouha(tokens):
    for i in range(1, len(tokens)):
        previous, ijou = tokens[i - 1:i + 1]
        pos = previous['pos']
        if (ijou['surface'] != '以上' or ijou['lemma'] != '以上'
                or ijou['pos'][:1] != ['名詞'] or not adjacent(previous, ijou)
                or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith('連体形')):
            continue
        end = ijou['span'][1]
        if i + 1 < len(tokens):
            wa = tokens[i + 1]
            if (adjacent(ijou, wa) and wa['surface'] == wa['lemma'] == 'は'
                    and wa['pos'][:2] == ['助詞', '係助詞']):
                end = wa['span'][1]
        yield ijou['span'][0], end


RULE = GrammarRule(id='ijiyouha', expression=IJIYOUHA_EXPRESSION,
                   reading='いじょうは', meaning='given that; now that',
                   match=match_ijiyouha, order=107)
RULES = (RULE,)
