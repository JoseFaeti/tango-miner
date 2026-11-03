"""Source-aligned benefactive auxiliaries after a verb's te-form."""
from src.GrammarConstants import TE_BENEFACTIVE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, polite_suffix_end, negative_suffix_end


def match_te_benefactive(tokens):
    for i in range(1, len(tokens) - 1):
        verb, connective, auxiliary = tokens[i - 1:i + 2]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(verb, connective) or not adjacent(connective, auxiliary)
                or auxiliary['pos'][:2] != ['動詞', '非自立可能']):
            continue
        lemma, surface = auxiliary['lemma'], auxiliary['surface']
        forms = {
            'あげる': ('あげる', 'あげ', 'あげ', 'あげ'),
            'くれる': ('くれる', 'くれ', 'くれ', 'くれ'),
            'もらう': ('もらう', 'もらっ', 'もらい', 'もらわ'),
        }
        if lemma not in forms:
            continue
        base, past, polite, negative = forms[lemma]
        end = i + 1
        if surface == base:
            pass
        elif surface == past and next_matches(tokens, end, 'た', '助動詞', 'た'):
            end += 1
        elif surface == polite and next_matches(tokens, end, 'ます', '助動詞'):
            end = polite_suffix_end(tokens, end + 1)
        elif surface == negative and next_matches(tokens, end, 'ない', '助動詞'):
            end = negative_suffix_end(tokens, end + 1, pos='助動詞', continuative=True, extended=True)
        else:
            continue
        if end is not None:
            yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    id='te_benefactive', expression=TE_BENEFACTIVE_EXPRESSION,
    reading='てあげる / てくれる / てもらう',
    meaning='do for someone; someone does for me; have someone do for me',
    match=match_te_benefactive,
)
