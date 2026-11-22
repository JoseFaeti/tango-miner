"""Attributive formal occasion: に際しての."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.niatatsute_nisaishite.index import match_niatatsute_nisaishite


def match_nisaishiteno(tokens):
    for start, end in match_niatatsute_nisaishite(tokens):
        for i, token in enumerate(tokens[:-1]):
            following = tokens[i + 1]
            if (i > 0 and token['span'][1] == end and token['surface'] == 'て'
                    and tokens[i - 1]['lemma'] == '際する'
                    and following['surface'] == 'の' and following['pos'][:1] == ['助詞']
                    and adjacent(token, following)):
                yield start, following['span'][1]


RULE = GrammarRule(id='nisaishiteno', expression='〜に際しての', reading='にさいしての',
                   meaning='on the occasion of; in connection with', match=match_nisaishiteno, order=11)
RULES = (RULE,)
