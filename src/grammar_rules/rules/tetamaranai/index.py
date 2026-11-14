"""An overwhelming feeling or urge expressed with てたまらない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, next_matches, polite_suffix_end

TETAMARANAI_EXPRESSION = '〜てたまらない'


def match_tetamaranai(tokens):
    for i in range(1, len(tokens) - 2):
        previous, connective, verb, suffix = tokens[i - 1:i + 3]
        if not all(adjacent(a, b) for a, b in zip(
                (previous, connective, verb), (connective, verb, suffix))):
            continue
        if connective['surface'] == 'て':
            if (connective['pos'][:2] != ['助詞', '接続助詞']
                    or previous['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                    or len(previous['pos']) < 6 or not previous['pos'][5].startswith('連用形')):
                continue
        elif connective['surface'] == 'で':
            if not (connective['pos'][:2] == ['助詞', '接続助詞']
                    and previous['pos'][:1] in (['動詞'], ['助動詞'])
                    and len(previous['pos']) >= 6 and previous['pos'][5].startswith('連用形')
                    or connective['pos'][:1] in (['助詞'], ['助動詞'])
                    and previous['pos'][:1] in (['名詞'], ['形状詞'])):
                continue
        else:
            continue
        if verb['pos'][:1] != ['動詞'] or verb['lemma'] not in ('たまる', '堪る'):
            continue
        end = None
        if verb['surface'] in ('たまら', '堪ら'):
            end = negative_suffix_end(tokens, i + 2, pos='助動詞', conditional=True)
            if (suffix['surface'] == 'なく' and suffix['lemma'] == 'ない'
                    and suffix['pos'][:1] == ['助動詞']
                    and next_matches(tokens, i + 2, 'て', '助詞', 'て')):
                end = i + 3
        elif verb['surface'] in ('たまり', '堪り'):
            end = polite_suffix_end(tokens, i + 2, forms=('negative',))
        if end is not None:
            yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=36, id='tetamaranai', expression=TETAMARANAI_EXPRESSION,
                   reading='てたまらない', meaning='unbearably; cannot help feeling',
                   match=match_tetamaranai)
RULES = (RULE,)
