"""Predicate-attached ように, preserving noun comparisons as lexical uses."""
YOU_NI_GRAMMAR_EXPRESSION = '〜ように'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.you_ni_naru.index import match_you_ni_naru
from src.grammar_rules.rules.you_ni_suru.index import match_you_ni_suru
from src.grammar_rules.rules.you_ni_iu_tanomu.index import match_you_ni_iu_tanomu
from src.grammar_rules.rules.you_ni_mieru.index import match_you_ni_mieru


RULE = GrammarRule(order=150,
    id='you_ni', expression=YOU_NI_GRAMMAR_EXPRESSION, reading='ように',
    meaning='so that; in order to; as; like; (in a request) to do something',
    match=lambda tokens: match_you_ni(tokens),
)


def match_you_ni(tokens):
    owned_starts = None
    for i in range(1, len(tokens) - 1):
        previous, token = tokens[i - 1:i + 1]
        if token['surface'] not in ('ように', 'よう'):
            continue
        if (not adjacent(previous, token)
                or previous['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(previous['pos']) < 6
                or not previous['pos'][5].startswith(('終止形', '連体形'))):
            continue
        if token['surface'] == 'ように' and token['lemma'] == 'ように':
            end = i
        elif (token['surface'] == 'よう' and token['lemma'] == 'よう'
              and token['pos'][:1] == ['形状詞'] and i + 1 < len(tokens)
              and adjacent(token, tokens[i + 1])
              and tokens[i + 1]['surface'] == 'に'
              and tokens[i + 1]['lemma'] == 'だ'
              and tokens[i + 1]['pos'][:1] == ['助動詞']):
            end = i + 1
        else:
            continue
        if not any(t['pos'][:1] != ['補助記号'] for t in tokens[end + 1:]):
            continue
        if owned_starts is None:
            owned_starts = {start for matcher in (match_you_ni_naru, match_you_ni_suru, match_you_ni_iu_tanomu, match_you_ni_mieru)
                            for start, _ in matcher(tokens)}
        if token['span'][0] not in owned_starts:
            yield token['span'][0], tokens[end]['span'][1]

RULES = (RULE,)
