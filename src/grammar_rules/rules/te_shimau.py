"""Rule for te_shimau."""
from src.GrammarConstants import TE_SHIMAU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import polite_suffix_end, has_next, next_matches, negative_suffix_end


RULE = GrammarRule(
    id='te_shimau', expression=TE_SHIMAU_GRAMMAR_EXPRESSION, reading='てしまう',
    meaning='finish doing; do completely; do unintentionally or with regret',
    match=lambda tokens: match_te_shimau(tokens),
)


def match_te_shimau(tokens):
    for i in range(1, len(tokens)):
        verb, first = tokens[i - 1:i + 1]
        if (not has_next(tokens, i - 1) or verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')):
            continue
        end = i
        if (first['surface'] in ('て', 'で') and first['pos'][:2] == ['助詞', '接続助詞']
                and has_next(tokens, i) and tokens[i + 1]['lemma'] in ('しまう', '仕舞う')
                and tokens[i + 1]['pos'][:2] == ['動詞', '非自立可能']):
            end += 1
            stem = tokens[end]
        elif first['lemma'] in ('ちゃう', 'じゃう') and first['pos'][:1] == ['助動詞']:
            stem = first
        else:
            continue
        surface = stem['surface']
        if surface in ('しまう', '仕舞う', 'ちゃう', 'じゃう'):
            pass
        elif surface in ('しまっ', '仕舞っ', 'ちゃっ', 'じゃっ'):
            if not (next_matches(tokens, end, 'た', '助動詞')
                    or next_matches(tokens, end, 'て', '助詞', 'て')):
                continue
            end += 1
        elif surface in ('しまい', '仕舞い', 'ちゃい', 'じゃい'):
            if not has_next(tokens, end) or tokens[end + 1]['lemma'] != 'ます' or tokens[end + 1]['pos'][:1] != ['助動詞']:
                continue
            end += 1
            end = polite_suffix_end(tokens, end)
            if end is None:
                continue
        elif surface in ('しまわ', '仕舞わ', 'ちゃわ', 'じゃわ'):
            if not has_next(tokens, end) or tokens[end + 1]['lemma'] != 'ない' or tokens[end + 1]['pos'][:1] != ['助動詞']:
                continue
            end = negative_suffix_end(tokens, end + 1, pos='助動詞', continuative=True, extended=True)
            if end is None:
                continue
        else:
            continue
        yield first['span'][0], tokens[end]['span'][1]
