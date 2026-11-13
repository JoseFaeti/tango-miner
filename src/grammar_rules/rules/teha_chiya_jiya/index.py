"""Verbal conditional or repeated-action ては and its contractions."""
TEHA_CHIYA_JIYA_EXPRESSION = '〜ては / 〜ちゃ / 〜じゃ'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_teha(tokens):
    for i in range(1, len(tokens)):
        anchor, first = tokens[i - 1:i + 1]
        if (anchor['pos'][:1] != ['動詞'] or len(anchor['pos']) < 6
                or not anchor['pos'][5].startswith('連用形')
                or not adjacent(anchor, first)
                or first['pos'][:2] != ['助詞', '接続助詞']):
            continue
        if first['surface'] in ('ては', 'では', 'ちゃ', 'じゃ'):
            yield first['span']
        elif (first['surface'] in ('て', 'で') and i + 1 < len(tokens)
              and adjacent(first, tokens[i + 1])
              and tokens[i + 1]['surface'] == 'は'
              and tokens[i + 1]['pos'][:1] == ['助詞']):
            yield first['span'][0], tokens[i + 1]['span'][1]


RULE = GrammarRule(order=176,
    id='teha_chiya_jiya', expression=TEHA_CHIYA_JIYA_EXPRESSION,
    reading='ては / ちゃ / じゃ', meaning='if; when; repeated action; forms negative constructions',
    match=match_teha,
)

RULES = (RULE,)
