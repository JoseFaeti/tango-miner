"""Reported or conventional judgment expressed with とされる."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end


def match_tosareru(tokens):
    for i in range(1, len(tokens) - 2):
        previous, to = tokens[i - 1:i + 1]
        pos = previous['pos']
        predicate = (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形')))
        nominal = pos[:1] in (['名詞'], ['代名詞'], ['形状詞'])
        if not ((predicate or nominal) and adjacent(previous, to)
                and to['surface'] == 'と' and to['pos'][:2] == ['助詞', '格助詞']
                and next_matches(tokens, i, 'する', '動詞', 'さ')
                and next_matches(tokens, i + 1, 'れる', '助動詞')):
            continue
        end = positive_verb_end(tokens, i + 2, lemma='れる', pos='助動詞',
                                base_forms={'れる'}, past_stems={'れ'},
                                polite_stems={'れ'}, te_stems={'れ'})
        if end is not None:
            yield to['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='tosareru', expression='〜とされる', reading='とされる',
                   meaning='is regarded as; it is considered that', match=match_tosareru)
RULES = (RULE,)
