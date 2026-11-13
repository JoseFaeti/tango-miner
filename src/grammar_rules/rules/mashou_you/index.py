"""Verb volitional and polite invitation forms."""
MASHOU_YOU_EXPRESSION = '〜ましょう / 〜よう'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_mashou_you(tokens):
    for i, token in enumerate(tokens):
        pos = token['pos']
        if (pos[:1] == ['動詞'] and len(pos) >= 6
                and pos[5].startswith('意志推量形')):
            yield token['span']
        elif (i and token['lemma'] == 'ます' and pos[:1] == ['助動詞']
              and token['surface'] in ('ましょう', 'ましょ')
              and adjacent(tokens[i - 1], token)):
            verb = tokens[i - 1]
            if (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                    and verb['pos'][5].startswith('連用形')):
                yield token['span']


RULE = GrammarRule(order=170, id='mashou_you', expression=MASHOU_YOU_EXPRESSION,
                   reading='ましょう / よう', meaning='let us; intend to do',
                   match=match_mashou_you)

RULES = (RULE,)
