"""Match the negative やむ construction after a verbal connective."""
TEYAMANAI_GRAMMAR_EXPRESSION = '〜てやまない'

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import has_next, negative_suffix_end


def match_teyamanai(tokens):
    for i in range(1, len(tokens) - 2):
        verb, connective, yamu = tokens[i - 1:i + 2]
        if not (has_next(tokens, i - 1) and has_next(tokens, i)
                and has_next(tokens, i + 1)
                and verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith('連用形')
                and connective['surface'] in ('て', 'で')
                and connective['pos'][:2] == ['助詞', '接続助詞']
                and yamu['surface'] in ('やま', '止ま', '已ま')
                and yamu['lemma'] in ('やむ', '止む', '已む')
                and yamu['pos'][:1] == ['動詞']):
            continue
        end = negative_suffix_end(tokens, i + 2, pos='助動詞', conditional=True)
        if end is not None:
            yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=160, id='teyamanai', expression=TEYAMANAI_GRAMMAR_EXPRESSION,
                   reading='てやまない', meaning='cannot stop feeling; earnestly and continually',
                   match=match_teyamanai)
RULES = (RULE,)
