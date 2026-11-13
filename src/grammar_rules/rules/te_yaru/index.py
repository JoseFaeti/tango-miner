"""Verb te-form followed by the benefactive or assertive auxiliary やる."""
TE_YARU_EXPRESSION = '〜てやる'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, positive_verb_end


def match_te_yaru(tokens):
    for i in range(1, len(tokens) - 1):
        verb, connective, auxiliary = tokens[i - 1:i + 2]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(verb, connective) or not adjacent(connective, auxiliary)
                or auxiliary['lemma'] != 'やる'
                or auxiliary['pos'][:2] != ['動詞', '非自立可能']):
            continue
        end = positive_verb_end(tokens, i + 1, lemma='やる',
                                base_forms={'やる', 'やろう'}, past_stems={'やっ'},
                                polite_stems={'やり'}, te_stems={'やっ'},
                                conditional_stems={'やれ'})
        if end is None and auxiliary['surface'] == 'やら' and i + 2 < len(tokens):
            suffix = tokens[i + 2]
            if adjacent(auxiliary, suffix) and suffix['lemma'] == 'ない' and suffix['pos'][:1] == ['助動詞']:
                end = negative_suffix_end(tokens, i + 2, pos='助動詞', continuative=True, extended=True)
        if end is not None:
            yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=42,
    id='te_yaru', expression=TE_YARU_EXPRESSION, reading='てやる',
    meaning='do something for someone; resolve to do something', match=match_te_yaru,
)

RULES = (RULE,)
