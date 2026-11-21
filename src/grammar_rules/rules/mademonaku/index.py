"""Verb + までもなく: without needing to do something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_mademonaku(tokens):
    for i in range(1, len(tokens) - 2):
        verb, made = tokens[i - 1:i + 1]
        naku = tokens[i + 2]
        if (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith(('終止形', '連体形'))
                and adjacent(verb, made)
                and made['surface'] == made['lemma'] == 'まで'
                and made['pos'][:1] == ['助詞']
                and next_matches(tokens, i, 'も', '助詞', 'も')
                and adjacent(tokens[i + 1], naku)
                and naku['surface'] == 'なく' and naku['lemma'] == 'ない'
                and naku['pos'][:1] == ['形容詞']):
            yield made['span'][0], naku['span'][1]


RULE = GrammarRule(id='mademonaku', expression='〜までもなく', reading='までもなく',
                   meaning='without needing to; needless to say',
                   match=match_mademonaku, order=79)
RULES = (RULE,)
