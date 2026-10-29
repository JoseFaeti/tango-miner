"""Source-aligned verb て-form + おく, including supported contractions."""
from src.GrammarConstants import TE_OKU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, polite_suffix_end, negative_suffix_end


def match_te_oku(tokens):
    for i in range(1, len(tokens)):
        verb, first = tokens[i - 1:i + 1]
        if (not adjacent(verb, first) or verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')):
            continue
        if (first['surface'] in ('て', 'で') and first['pos'][:2] == ['助詞', '接続助詞']
                and i + 1 < len(tokens) and adjacent(first, tokens[i + 1])
                and tokens[i + 1]['lemma'] == 'おく'
                and tokens[i + 1]['pos'][:2] == ['動詞', '非自立可能']):
            end = i + 1
            prefix = 'お'
        elif first['lemma'] in ('とく', 'どく') and first['pos'][:1] == ['助動詞']:
            end = i
            prefix = first['lemma'][0]
        else:
            continue

        stem = tokens[end]['surface']
        if stem in (prefix + 'く', prefix + 'こう'):
            pass
        elif stem == prefix + 'い':
            if not (next_matches(tokens, end, 'た', '助動詞', 'た')
                    or next_matches(tokens, end, 'た', '助動詞', 'たら')
                    or next_matches(tokens, end, 'て', '助詞', 'て')):
                continue
            end += 1
        elif stem == prefix + 'き':
            if not next_matches(tokens, end, 'ます', '助動詞'):
                continue
            end = polite_suffix_end(tokens, end + 1)
            if end is None:
                continue
        elif stem == prefix + 'か':
            if not next_matches(tokens, end, 'ない', '助動詞'):
                continue
            end = negative_suffix_end(tokens, end + 1, pos='助動詞', continuative=True, extended=True)
            if end is None:
                continue
        elif stem == prefix + 'け':
            if not next_matches(tokens, end, 'ば', '助詞', 'ば'):
                continue
            end += 1
        else:
            continue
        yield first['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    id='te_oku', expression=TE_OKU_GRAMMAR_EXPRESSION, reading='ておく',
    meaning=('Grammar: verb て-form + おく can describe doing something in advance '
             'or leaving a resulting state. Colloquial forms include とく and どく; '
             'context determines the meaning.'),
    match=match_te_oku,
)
