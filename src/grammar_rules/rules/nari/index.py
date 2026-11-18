"""Immediate succession after a dictionary-form verb."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NARI_EXPRESSION = '〜なり'


def match_nari(tokens):
    for i in range(1, len(tokens)):
        verb, marker = tokens[i - 1:i + 1]
        if not (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith(('終止形', '連体形'))
                and marker['surface'] == 'なり'
                and marker['pos'][:2] == ['助詞', '接続助詞']
                and adjacent(verb, marker)):
            continue
        # VなりVなり is a choice, even when Sudachi tags the first as conjunctive.
        j = i + 1
        if j < len(tokens) and tokens[j]['surface'] in ('、', '，'):
            j += 1
        if (j + 1 < len(tokens) and tokens[j]['pos'][:1] == ['動詞']
                and tokens[j + 1]['surface'] == 'なり'
                and tokens[j + 1]['pos'][:1] == ['助詞']
                and adjacent(tokens[j], tokens[j + 1])):
            continue
        yield marker['span']


RULE = GrammarRule(order=51, id='nari', expression=NARI_EXPRESSION,
                   reading='なり', meaning='as soon as; the moment that',
                   match=match_nari)
RULES = (RULE,)
