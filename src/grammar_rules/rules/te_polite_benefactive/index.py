"""Polite benefactive auxiliaries following a verb's te-form."""
TE_POLITE_BENEFACTIVE_EXPRESSION = '〜ていただく / 〜てくださる / 〜て差し上げる'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end, positive_verb_end


FORMS = {
    'いただく': ('いただく', 'いただい', 'いただき', 'いただか', 'いただけ', 'いただこう'),
    '頂く': ('頂く', '頂い', '頂き', '頂か', '頂け', '頂こう'),
    'くださる': ('くださる', 'くださっ', 'ください', 'くださら', 'くだされ', 'くださろう'),
    '下さる': ('下さる', '下さっ', '下さい', '下さら', '下され', '下さろう'),
    '差し上げる': ('差し上げる', '差し上げ', '差し上げ', '差し上げ', '差し上げれ', '差し上げよう'),
    'さしあげる': ('さしあげる', 'さしあげ', 'さしあげ', 'さしあげ', 'さしあげれ', 'さしあげよう'),
}


def match_te_polite_benefactive(tokens):
    for i in range(1, len(tokens) - 1):
        verb, connective, auxiliary = tokens[i - 1:i + 2]
        lemma = auxiliary['lemma']
        if (lemma not in FORMS or connective['surface'] not in ('て', 'で')
                or verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(verb, connective) or not adjacent(connective, auxiliary)
                or auxiliary['pos'][:1] != ['動詞']
                or (lemma not in ('差し上げる', 'さしあげる')
                    and auxiliary['pos'][:2] != ['動詞', '非自立可能'])):
            continue
        base, past, polite, negative, conditional, volitional = FORMS[lemma]
        end = positive_verb_end(tokens, i + 1, lemma=lemma,
                                base_forms={base, volitional}, past_stems={past},
                                polite_stems={polite}, te_stems={past},
                                conditional_stems={conditional})
        if (end is None and lemma in ('いただく', '頂く') and auxiliary['surface'] == past
                and next_matches(tokens, i + 1, 'で', '助詞', 'で')):
            end = i + 2
        if end is None and auxiliary['surface'] == polite and next_matches(tokens, i + 1, 'ます', '助動詞'):
            end = polite_suffix_end(tokens, i + 2)
        elif end is None and auxiliary['surface'] == negative and next_matches(tokens, i + 1, 'ない', '助動詞'):
            end = negative_suffix_end(tokens, i + 2, pos='助動詞', continuative=True, extended=True)
        if end is not None:
            yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=116,
    id='te_polite_benefactive', expression=TE_POLITE_BENEFACTIVE_EXPRESSION,
    reading='ていただく / てくださる / てさしあげる',
    meaning='humbly receive an action; someone respectfully does for me; humbly do for someone',
    match=match_te_polite_benefactive,
)

RULES = (RULE,)
