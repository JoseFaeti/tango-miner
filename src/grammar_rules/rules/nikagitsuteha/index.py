"""Restrict a statement to a particular case with に限っては."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nikagitsute.index import match_nikagitsute


def match_nikagitsuteha(tokens):
    for start, end in match_nikagitsute(tokens):
        for i in range(1, len(tokens)):
            te, ha = tokens[i - 1:i + 1]
            if (te['span'][1] == end and ha['surface'] == 'は'
                    and ha['pos'][:1] == ['助詞'] and adjacent(te, ha)):
                yield start, ha['span'][1]
                break


RULE = GrammarRule(order=2, id='nikagitsuteha', expression='〜に限っては',
                   reading='にかぎっては', meaning='only in the case of; as for',
                   match=match_nikagitsuteha)
RULES = (RULE,)
