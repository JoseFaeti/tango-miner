"""Literary prohibition with verbal べからず."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_bekarazu(tokens):
    for i in range(1, len(tokens) - 1):
        verb, beka, negative = tokens[i - 1:i + 2]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith(('終止形', '連体形'))
                or (beka['surface'], beka['lemma'], beka['pos'][:1]) != ('べから', 'べし', ['助動詞'])
                or (negative['surface'], negative['lemma'], negative['pos'][:1]) not in (
                    ('ず', 'ず', ['助動詞']), ('ざる', 'ぬ', ['助動詞']))
                or not adjacent(verb, beka) or not adjacent(beka, negative)):
            continue
        yield beka['span'][0], negative['span'][1]


RULE = GrammarRule(id='bekarazu', expression='〜べからず', reading='べからず',
                   meaning='must not; do not (literary prohibition)', match=match_bekarazu, order=131)
RULES = (RULE,)
