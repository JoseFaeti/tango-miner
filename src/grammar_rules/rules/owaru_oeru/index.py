"""Completion expressed by a verb compound with 終わる or 終える."""
OWARU_OERU_EXPRESSION = '〜終わる / 〜終える'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end


def match_owaru_oeru(tokens):
    for i, token in enumerate(tokens):
        lemma, surface = token['lemma'], token['surface']
        if token['pos'][:1] != ['動詞'] or not isinstance(lemma, str):
            continue
        suffix = next((s for s in ('終わる', 'おわる', '終える', 'おえる')
                         if lemma.endswith(s)), None)
        if suffix is None:
            continue
        start = token['span'][0]
        if lemma == suffix:
            if i == 0:
                continue
            previous = tokens[i - 1]
            if not (adjacent(previous, token) and previous['pos'][:1] == ['動詞']
                    and len(previous['pos']) >= 6 and previous['pos'][5].startswith('連用形')):
                continue
            start = previous['span'][0]
        stem = suffix[:-1]
        godan = suffix in ('終わる', 'おわる')
        end = None
        if surface.endswith(suffix):
            end = i
        elif surface.endswith(stem + ('っ' if godan else '')) and (
                next_matches(tokens, i, 'た', '助動詞', 'た')
                or next_matches(tokens, i, 'て', '助詞', 'て')):
            end = i + 1
        elif surface.endswith(stem + ('り' if godan else '')) and next_matches(tokens, i, 'ます', '助動詞'):
            end = polite_suffix_end(tokens, i + 1)
        elif surface.endswith(stem + ('ら' if godan else '')) and next_matches(tokens, i, 'ない', '助動詞'):
            end = negative_suffix_end(tokens, i + 1, pos='助動詞', extended=True)
        elif surface.endswith(stem + 'れ') and next_matches(tokens, i, 'ば', '助詞', 'ば'):
            end = i + 1
        if end is not None:
            yield start, tokens[end]['span'][1]


RULE = GrammarRule(order=179, id='owaru_oeru', expression=OWARU_OERU_EXPRESSION,
                   reading='おわる / おえる', meaning='finish doing',
                   match=match_owaru_oeru)

RULES = (RULE,)
