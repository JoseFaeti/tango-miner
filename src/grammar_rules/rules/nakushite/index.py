"""Noun + なくして: without (formal)."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_nakushite(tokens):
    for i in range(1, len(tokens) - 2):
        noun, negative = tokens[i - 1:i + 1]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and adjacent(noun, negative)
                and negative['surface'] == 'なく' and negative['lemma'] == 'ない'
                and negative['pos'][:1] == ['形容詞']
                and next_matches(tokens, i, 'する', '動詞', 'し')
                and next_matches(tokens, i + 1, 'て', '助詞', 'て')):
            yield negative['span'][0], tokens[i + 2]['span'][1]


RULE = GrammarRule(id='nakushite', expression='〜なくして', reading='なくして',
                   meaning='without (formal)', match=match_nakushite, order=1)
RULES = (RULE,)
