"""Conditional or topic-based なら with an aligned antecedent."""
NARA_GRAMMAR_EXPRESSION = '〜なら'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nara(tokens):
    for i in range(1, len(tokens) - 1):
        previous, conditional = tokens[i - 1:i + 1]
        pos = previous['pos']
        if (conditional['surface'] != 'なら' or conditional['lemma'] != 'だ'
                or conditional['pos'][:1] != ['助動詞']
                or len(conditional['pos']) < 6
                or not conditional['pos'][5].startswith('仮定形')
                or not adjacent(previous, conditional)):
            continue
        predicate = (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形')))
        nominal = pos[:1] in (['名詞'], ['代名詞'], ['形状詞'])
        nominalizer = (previous['surface'] == 'の' and pos[:2] == ['助詞', '準体助詞']
                       and i >= 2 and adjacent(tokens[i - 2], previous)
                       and tokens[i - 2]['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                       and len(tokens[i - 2]['pos']) >= 6
                       and tokens[i - 2]['pos'][5].startswith('連体形'))
        if not (predicate or nominal or nominalizer):
            continue
        if not any(t['pos'][:1] != ['補助記号'] for t in tokens[i + 1:]):
            continue
        yield tuple(conditional['span'])


RULE = GrammarRule(order=137, id='conditional_nara', expression=NARA_GRAMMAR_EXPRESSION,
                   reading='なら', meaning='if; as for; supposing that', match=match_nara)

RULES = (RULE,)
