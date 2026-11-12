"""Scheduled plans expressed with a finite 予定 copula."""
from src.GrammarConstants import YOTEIDA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_yoteida(tokens):
    for i in range(1, len(tokens) - 1):
        previous, noun, copula = tokens[i - 1:i + 2]
        pos = previous['pos']
        verb = (pos[:1] == ['動詞'] and len(pos) >= 6
                and pos[5].startswith('連体形'))
        negative = (i >= 2 and previous['lemma'] == 'ない'
                    and previous['surface'] == 'ない'
                    and pos[:1] in (['助動詞'], ['形容詞'])
                    and tokens[i - 2]['pos'][:1] == ['動詞']
                    and adjacent(tokens[i - 2], previous))
        nominal = (i >= 2 and previous['surface'] == 'の'
                   and pos[:1] == ['助詞']
                   and tokens[i - 2]['pos'][:1] == ['名詞']
                   and adjacent(tokens[i - 2], previous))
        if not (verb or negative or nominal):
            continue
        if (noun['surface'] != '予定' or noun['lemma'] != '予定'
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


RULE = GrammarRule(id='yoteida', expression=YOTEIDA_EXPRESSION,
                   reading='よていだ', meaning='plan to; be scheduled to', match=match_yoteida)
