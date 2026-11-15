"""Past/completive auxiliary after a continuative predicate."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TA_GRAMMAR_EXPRESSION = '〜た'


def match_ta(tokens):
    for i in range(1, len(tokens)):
        predicate, past = tokens[i - 1:i + 1]
        if ((past['surface'], past['lemma']) not in (('た', 'た'), ('だ', 'だ'))
                or past['pos'][:1] != ['助動詞']
                or len(past['pos']) < 6
                or not past['pos'][5].startswith(('終止形', '連体形'))
                or predicate['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(predicate['pos']) < 6
                or not predicate['pos'][5].startswith('連用形')
                or not adjacent(predicate, past)):
            continue
        yield tuple(past['span'])


RULE = GrammarRule(order=167, id='ta', expression=TA_GRAMMAR_EXPRESSION,
                   reading='た', meaning='past or completed event', match=match_ta)
RULES = (RULE,)
