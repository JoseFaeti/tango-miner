"""Affirmative に堪える / に耐える construction."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, positive_verb_end


def match_nitaeru(tokens):
    for i in range(1, len(tokens) - 1):
        before, ni, verb = tokens[i - 1:i + 2]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or ni['surface'] != 'に'
                or not (ni['pos'][:1] == ['助詞']
                        or (ni['pos'][:1] == ['助動詞'] and ni['lemma'] == 'だ'))
                or not adjacent(before, ni) or not adjacent(ni, verb)
                or verb['lemma'] not in ('堪える', '耐える', 'たえる')):
            continue
        end = positive_verb_end(
            tokens, i + 1, lemma=verb['lemma'],
            base_forms=('堪える', '耐える', 'たえる'),
            past_stems=('堪え', '耐え', 'たえ'),
            polite_stems=('堪え', '耐え', 'たえ'),
            te_stems=('堪え', '耐え', 'たえ'),
            conditional_stems=('堪えれ', '耐えれ', 'たえれ'))
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=3, id='nitaeru', expression='〜に堪える',
                   reading='にたえる', meaning='withstand; be worthy of', match=match_nitaeru)
RULES = (RULE,)
