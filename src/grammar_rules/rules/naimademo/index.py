"""Concession of a higher degree: ないまでも."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches


def match_naimademo(tokens):
    for i, negative in enumerate(tokens):
        if (negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] not in (['助動詞'], ['形容詞'])
                or not next_matches(tokens, i, 'まで', '助詞', 'まで')
                or not next_matches(tokens, i + 1, 'も', '助詞', 'も')):
            continue
        yield negative['span'][0], tokens[i + 2]['span'][1]


RULE = GrammarRule(id='naimademo', expression='〜ないまでも',
                   reading='ないまでも', meaning='even if not; though not to that extent',
                   match=match_naimademo, order=80)
RULES = (RULE,)
