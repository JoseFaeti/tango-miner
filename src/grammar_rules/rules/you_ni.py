"""Predicate-attached ように, preserving noun comparisons as lexical uses."""
from src.GrammarConstants import YOU_NI_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


RULE = GrammarRule(
    id='you_ni', expression=YOU_NI_GRAMMAR_EXPRESSION, reading='ように',
    meaning='so that; in order to; as; like; (in a request) to do something',
    match=lambda tokens: match_you_ni(tokens),
)


def match_you_ni(tokens):
    for i in range(1, len(tokens) - 1):
        previous, token = tokens[i - 1:i + 1]
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
        yield token['span'][0], tokens[end]['span'][1]
