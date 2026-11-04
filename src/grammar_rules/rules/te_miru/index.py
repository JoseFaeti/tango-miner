"""Source-aligned verb te-form + non-independent miru (try doing)."""
from src.GrammarConstants import TE_MIRU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end


def match_te_miru(tokens):
    for i in range(1, len(tokens)):
        verb, connective = tokens[i - 1:i + 1]
        if (not adjacent(verb, connective)
                or verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6
                or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or i + 1 >= len(tokens) or not adjacent(connective, tokens[i + 1])):
            continue
        miru = tokens[i + 1]
        if miru['lemma'] not in ('みる', '見る') or miru['pos'][:2] != ['動詞', '非自立可能']:
            continue
        end = None
        if miru['surface'] in ('みる', '見る'):
            end = i + 1
        elif miru['surface'] in ('み', '見') and i + 2 < len(tokens) and adjacent(miru, tokens[i + 2]):
            suffix = tokens[i + 2]
            if suffix['lemma'] == 'た' and suffix['pos'][:1] == ['助動詞'] and suffix['surface'] == 'た':
                end = i + 2
            elif suffix['lemma'] == 'ます' and suffix['pos'][:1] == ['助動詞']:
                end = polite_suffix_end(tokens, i + 2, forms=('positive',))
            elif suffix['lemma'] == 'ない' and suffix['pos'][:1] == ['助動詞']:
                end = negative_suffix_end(tokens, i + 2, pos='助動詞', continuative=True, extended=True)
        if end is not None:
            yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    id='te_miru', expression=TE_MIRU_GRAMMAR_EXPRESSION, reading='てみる',
    meaning='try doing; have a go at; do and see what happens',
    match=match_te_miru,
)
