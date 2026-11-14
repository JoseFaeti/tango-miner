"""Combined concessive としても / にしても identity."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.nishitemo.index import match_nishitemo
from src.grammar_rules.rules.toshitemo.index import match_toshitemo

TOSHITEMO_NISHITEMO_EXPRESSION = '〜としても / 〜にしても'


def match_toshitemo_nishitemo(tokens):
    yield from sorted(set(match_toshitemo(tokens)) | set(match_nishitemo(tokens)))


RULE = GrammarRule(order=16, id='toshitemo_nishitemo',
                   expression=TOSHITEMO_NISHITEMO_EXPRESSION,
                   reading='としても / にしても', meaning='even if; even assuming',
                   match=match_toshitemo_nishitemo)
RULES = (RULE,)
