"""Verb-attached intention with a finite つもり copula."""
from src.GrammarConstants import TSUMORIDA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_tsumorida(tokens):
    for i in range(1, len(tokens) - 1):
        previous, noun, copula = tokens[i - 1:i + 2]
        if noun['surface'] not in ('つもり', '積もり'):
            continue
        pos = previous['pos']
        verb = (pos[:1] == ['動詞'] and len(pos) >= 6
                and pos[5].startswith('連体形'))
        negative = (i >= 2 and previous['lemma'] == 'ない'
                    and previous['surface'] == 'ない'
                    and pos[:1] in (['助動詞'], ['形容詞'])
                    and tokens[i - 2]['pos'][:1] == ['動詞']
                    and adjacent(tokens[i - 2], previous))
        if not (verb or negative):
            continue
        if (noun['surface'] not in ('つもり', '積もり')
                or noun['lemma'] not in ('つもり', '積もり')
                or noun['pos'][:1] != ['名詞'] or not adjacent(previous, noun)
                or not adjacent(noun, copula) or copula['pos'][:1] != ['助動詞']):
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 1
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            end = i + 2
        else:
            continue
        yield noun['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='tsumorida', expression=TSUMORIDA_EXPRESSION,
                   reading='つもりだ', meaning='intend to; plan to', match=match_tsumorida)
