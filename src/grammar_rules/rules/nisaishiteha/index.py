"""Topicalized formal occasion: に際しては."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.niatatsute_nisaishite.index import (
    match_niatatsute_nisaishite,
)


def match_nisaishiteha(tokens):
    for start, end in match_niatatsute_nisaishite(tokens):
        for i, token in enumerate(tokens[:-1]):
            following = tokens[i + 1]
            if (token['span'][1] == end and token['surface'] == 'て'
                    and tokens[i - 1]['lemma'] == '際する'
                    and following['surface'] == 'は' and following['pos'][:1] == ['助詞']
                    and adjacent(token, following)):
                yield start, following['span'][1]


RULE = GrammarRule(id='nisaishiteha', expression='〜に際しては', reading='にさいしては',
                   meaning='on the occasion of; when', match=match_nisaishiteha, order=11)
RULES = (RULE,)
