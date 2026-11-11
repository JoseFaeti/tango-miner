"""Source-aligned causative followed by passive auxiliaries."""
from src.GrammarConstants import SASERARERU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

RULE = GrammarRule(id='saserareru', expression=SASERARERU_EXPRESSION,
                   reading='させられる', meaning='be made to do; be forced to do',
                   match=lambda tokens: match_saserareru(tokens))


def match_saserareru(tokens):
    for i in range(1, len(tokens) - 1):
        verb, causative, passive = tokens[i - 1:i + 2]
        if (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith(('未然形', '連用形'))
                and causative['pos'][:1] == ['助動詞']
                and causative['lemma'] in ('せる', 'させる')
                and causative['surface'] in ('せ', 'させ')
                and passive['pos'][:1] == ['助動詞']
                and passive['lemma'] == 'られる'
                and passive['surface'] in ('られる', 'られ', 'られれ', 'られよ')
                and adjacent(verb, causative) and adjacent(causative, passive)):
            yield causative['span'][0], passive['span'][1]
