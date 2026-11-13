"""A consequence or implication expressed with ということになる."""
TOIUKOTONINARU_EXPRESSION = '〜ということになる'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end


def match_toiukotoninaru(tokens):
    for i in range(1, len(tokens) - 3):
        previous, particle, verb, koto = tokens[i - 1:i + 3]
        pos = previous['pos']
        if (len(pos) < 6 or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not pos[5].startswith(('終止形', '連体形', '意志推量形'))
                or particle['surface'] not in ('と', 'って') or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いう', '言う') or verb['lemma'] not in ('いう', '言う')
                or verb['pos'][:1] != ['動詞']
                or not all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 2], tokens[i:i + 3]))):
            continue
        if koto['surface'] == 'ことになる' and koto['lemma'] == 'ことになる':
            end = i + 2
        elif (koto['surface'] in ('こと', '事') and koto['pos'][:2] == ['名詞', '普通名詞']
              and next_matches(tokens, i + 2, 'に', '助詞', 'に')
              and next_matches(tokens, i + 3, 'なる', '動詞')):
            end = positive_verb_end(tokens, i + 4, lemma='なる', base_forms={'なる'},
                                    past_stems={'なっ'}, polite_stems={'なり'})
            if end is None:
                continue
        else:
            continue
        yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=88, id='toiukotoninaru', expression=TOIUKOTONINARU_EXPRESSION,
                   reading='ということになる', meaning='that means; it follows that; it turns out that',
                   match=match_toiukotoninaru)

RULES = (RULE,)
