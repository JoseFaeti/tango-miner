"""Negative predicate + ことには, expressing a necessary condition."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches


def match_naikotoniha(tokens):
    for i, negative in enumerate(tokens):
        if (negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] not in (['助動詞'], ['形容詞'])
                or not next_matches(tokens, i, 'こと', '名詞', 'こと')
                or not next_matches(tokens, i + 1, 'に', '助詞', 'に')
                or not next_matches(tokens, i + 2, 'は', '助詞', 'は')):
            continue
        yield negative['span'][0], tokens[i + 3]['span'][1]


RULE = GrammarRule(id='naikotoniha', expression='〜ないことには',
                   reading='ないことには', meaning='unless; without doing so',
                   match=match_naikotoniha, order=80)
RULES = (RULE,)
