"""Changes of state with an adjective or nominal predicate."""
KUNARU_NINARU_EXPRESSION = '〜くなる / 〜になる'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end, negative_predicate_end


def match_kunaru_ninaru(tokens):
    for i in range(1, len(tokens)):
        before, naru = tokens[i - 1:i + 1]
        if not adjacent(before, naru) or naru['lemma'] != 'なる' or naru['pos'][:1] != ['動詞']:
            continue
        pos = before['pos']
        if (pos[:1] == ['形容詞'] and before['surface'].endswith('く')
                and len(pos) >= 6 and pos[5].startswith('連用形')):
            start = i - 1
        elif (before['surface'] == 'に' and before['lemma'] in ('に', 'だ')
              and before['pos'][:1] in (['助詞'], ['助動詞']) and i >= 2
              and adjacent(tokens[i - 2], before)
              and tokens[i - 2]['pos'][:1] in (['名詞'], ['形状詞'])
              and tokens[i - 2]['lemma'] not in ('こと', 'よう', 'そう')):
            start = i - 1
        else:
            continue
        end = positive_verb_end(tokens, i, lemma='なる', base_forms={'なる'},
                                past_stems={'なっ'}, polite_stems={'なり'},
                                te_stems={'なっ'}, conditional_stems={'なれ'})
        if end is None:
            end = negative_predicate_end(tokens, i, plain_lemma='なる',
                                         plain_stems={'なら'}, polite_lemma='なる',
                                         polite_stems={'なり'})
        if end is not None:
            yield tokens[start]['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=21, id='kunaru_ninaru', expression=KUNARU_NINARU_EXPRESSION,
                   reading='くなる / になる', meaning='become; change state',
                   match=match_kunaru_ninaru)

RULES = (RULE,)
