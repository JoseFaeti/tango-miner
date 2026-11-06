"""Source-aligned predicate expectation followed by a finite はず copula."""
from src.GrammarConstants import HAZU_DA_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_hazu_da(tokens):
    for i in range(1, len(tokens) - 1):
        previous, hazu, copula = tokens[i - 1:i + 2]
        if hazu['surface'] != 'はず':
            continue
        if not adjacent(previous, hazu) or not adjacent(hazu, copula):
            continue
        if hazu['surface'] != 'はず' or hazu['lemma'] != 'はず' or hazu['pos'][:2] != ['名詞', '普通名詞']:
            continue
        pos = previous['pos']
        predicate = (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形')))
        if not predicate or copula['pos'][:1] != ['助動詞']:
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 1
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            end = i + 2
        else:
            continue
        yield hazu['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='hazu_da', expression=HAZU_DA_GRAMMAR_EXPRESSION,
                   reading='はずだ', meaning='should be; expected to; must be', match=match_hazu_da)
