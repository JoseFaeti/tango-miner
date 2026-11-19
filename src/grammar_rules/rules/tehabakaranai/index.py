"""Verb-te + はばからない: do something openly without hesitation."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end

TEHABAKARANAI_EXPRESSION = '〜てはばからない'


def match_tehabakaranai(tokens):
    for i in range(1, len(tokens) - 2):
        verb, te, habakaru, suffix = tokens[i - 1:i + 3]
        if not (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith('連用形')
                and te['surface'] in ('て', 'で') and te['pos'][:2] == ['助詞', '接続助詞']
                and habakaru['lemma'] in ('はばかる', '憚る')
                and habakaru['pos'][:1] == ['動詞']
                and all(adjacent(a, b) for a, b in ((verb, te), (te, habakaru), (habakaru, suffix)))):
            continue
        end = None
        if habakaru['surface'] in ('はばから', '憚ら'):
            end = negative_suffix_end(tokens, i + 2, pos='助動詞', continuative=True, conditional=True)
        elif habakaru['surface'] in ('はばかり', '憚り'):
            end = polite_suffix_end(tokens, i + 2, forms=('negative',))
        if end is not None:
            yield te['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='tehabakaranai', expression=TEHABAKARANAI_EXPRESSION,
                   reading='てはばからない', meaning='do openly without hesitation',
                   match=match_tehabakaranai, order=1)
RULES = (RULE,)
