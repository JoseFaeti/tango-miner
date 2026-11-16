"""Formal conjecture with copular で and volitional あろう."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_dearou(tokens):
    for i in range(1, len(tokens) - 1):
        previous, de, arou = tokens[i - 1:i + 2]
        if (previous['pos'][:1] not in (['空白'], ['補助記号'])
                and adjacent(previous, de) and adjacent(de, arou)
                and (de['surface'], de['lemma'], de['pos'][:1]) == ('で', 'だ', ['助動詞'])
                and (arou['surface'], arou['lemma'], arou['pos'][:1]) == ('あろう', 'ある', ['動詞'])):
            yield de['span'][0], arou['span'][1]


RULE = GrammarRule(id='dearou', expression='〜であろう', reading='であろう',
                   meaning='probably is; would be (formal conjecture)', match=match_dearou)
RULES = (RULE,)
