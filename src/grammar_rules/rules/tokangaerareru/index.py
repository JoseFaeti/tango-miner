"""Inference expressed with と考えられる."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end


def match_tokangaerareru(tokens):
    for i in range(1, len(tokens) - 2):
        previous, to = tokens[i - 1:i + 1]
        pos = previous['pos']
        predicate = (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形')))
        noun = pos[:1] == ['名詞']
        if not ((predicate or noun) and adjacent(previous, to)
                and to['surface'] == 'と' and to['pos'][:2] == ['助詞', '格助詞']
                and next_matches(tokens, i, '考える', '動詞', '考え')
                and next_matches(tokens, i + 1, 'られる', '助動詞')):
            continue
        end = positive_verb_end(tokens, i + 2, lemma='られる', pos='助動詞',
                                base_forms={'られる'}, past_stems={'られ'},
                                polite_stems={'られ'})
        if end is not None:
            yield to['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='tokangaerareru', expression='〜と考えられる', reading='とかんがえられる',
                   meaning='it is considered; it is thought that', match=match_tokangaerareru)
RULES = (RULE,)
