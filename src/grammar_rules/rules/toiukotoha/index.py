"""Implication introduced by ということは."""
from src.GrammarConstants import TOIUKOTOHA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_toiukotoha(tokens):
    for i in range(1, len(tokens) - 3):
        previous, particle, verb, koto, topic = tokens[i - 1:i + 4]
        pos = previous['pos']
        if (len(pos) < 6 or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not pos[5].startswith(('終止形', '連体形', '意志推量形'))
                or particle['surface'] not in ('と', 'って') or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いう', '言う') or verb['lemma'] not in ('いう', '言う')
                or verb['pos'][:1] != ['動詞']
                or koto['surface'] not in ('こと', '事') or koto['pos'][:2] != ['名詞', '普通名詞']
                or topic['surface'] != 'は' or topic['pos'][:1] != ['助詞']
                or next_matches(tokens, i + 3, 'ない', '形容詞', 'ない')
                or not all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        yield particle['span'][0], topic['span'][1]


RULE = GrammarRule(id='toiukotoha', expression=TOIUKOTOHA_EXPRESSION,
                   reading='ということは', meaning='that means; if that is the case',
                   match=match_toiukotoha)
