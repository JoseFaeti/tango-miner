"""Continual change expressed by a verb followed by 一方だ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_itsupouda(tokens):
    for i in range(1, len(tokens) - 1):
        verb, noun, copula = tokens[i - 1:i + 2]
        pos = verb['pos']
        if not (pos[:1] == ['動詞'] and len(pos) >= 6
                and pos[5].startswith(('終止形', '連体形'))
                and noun['surface'] == noun['lemma'] == '一方'
                and noun['pos'][:1] == ['名詞']
                and adjacent(verb, noun) and adjacent(noun, copula)
                and copula['pos'][:1] == ['助動詞']):
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 1
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            end = i + 2
        else:
            continue
        yield noun['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='itsupouda', expression='〜一方だ', reading='いっぽうだ',
                   meaning='continue to change in one direction', match=match_itsupouda)
RULES = (RULE,)
