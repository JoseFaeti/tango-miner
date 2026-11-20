"""Questions and deliberation about what one should do."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_bekika(tokens):
    for i in range(1, len(tokens) - 1):
        verb, beki, ka = tokens[i - 1:i + 2]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith(('終止形', '連体形'))
                or (beki['surface'], beki['lemma'], beki['pos'][:1]) != ('べき', 'べし', ['助動詞'])
                or (ka['surface'], ka['lemma'], ka['pos'][:1]) != ('か', 'か', ['助詞'])
                or not adjacent(verb, beki) or not adjacent(beki, ka)):
            continue
        yield beki['span'][0], ka['span'][1]


RULE = GrammarRule(id='bekika', expression='〜べきか', reading='べきか',
                   meaning='whether one should; what one should do', match=match_bekika, order=132)
RULES = (RULE,)
