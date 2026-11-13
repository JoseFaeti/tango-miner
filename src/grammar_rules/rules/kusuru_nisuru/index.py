"""Making something a state, or choosing a noun."""
KUSURU_NISURU_EXPRESSION = '〜くする / 〜にする'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, positive_verb_end, negative_predicate_end


def match_kusuru_nisuru(tokens):
    for i in range(1, len(tokens)):
        before, suru = tokens[i - 1:i + 1]
        if not adjacent(before, suru) or suru['lemma'] != 'する' or suru['pos'][:1] != ['動詞']:
            continue
        pos = before['pos']
        if (pos[:1] == ['形容詞'] and before['surface'].endswith('く')
                and len(pos) >= 6 and pos[5].startswith('連用形')):
            start = i - 1
        elif (before['surface'] == 'に' and before['lemma'] in ('に', 'だ')
              and pos[:1] in (['助詞'], ['助動詞']) and i >= 2
              and adjacent(tokens[i - 2], before)
              and tokens[i - 2]['pos'][:1] in (['名詞'], ['形状詞'])
              and tokens[i - 2]['lemma'] not in ('こと', 'よう', 'そう')):
            start = i - 1
        else:
            continue
        end = positive_verb_end(tokens, i, lemma='する', base_forms={'する'},
                                past_stems={'し'}, polite_stems={'し'},
                                te_stems={'し'}, conditional_stems={'すれ'})
        if end is None:
            end = negative_predicate_end(tokens, i, plain_lemma='する',
                                         plain_stems={'し'}, polite_lemma='する',
                                         polite_stems={'し'})
        if end is not None:
            yield tokens[start]['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=20, id='kusuru_nisuru', expression=KUSURU_NISURU_EXPRESSION,
                   reading='くする / にする', meaning='make; choose',
                   match=match_kusuru_nisuru)

RULES = (RULE,)
