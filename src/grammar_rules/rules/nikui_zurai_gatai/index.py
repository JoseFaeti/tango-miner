"""Difficulty expressed by adjectival suffixes on a verb stem."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end, next_matches


NIKUI_ZURAI_GATAI_EXPRESSION = '〜にくい / 〜づらい / 〜がたい'
RULE = GrammarRule(
    order=110, id='nikui_zurai_gatai', expression=NIKUI_ZURAI_GATAI_EXPRESSION,
    reading='にくい / づらい / がたい',
    meaning='difficult to do; hard to do (physically, emotionally, or inherently)',
    match=lambda tokens: match_nikui_zurai_gatai(tokens),
)


def match_nikui_zurai_gatai(tokens):
    for i in range(1, len(tokens)):
        verb, suffix = tokens[i - 1:i + 1]
        lemma = suffix['lemma']
        if not (lemma in ('にくい', '難い', 'づらい', '辛い', 'がたい')
                and adjacent(verb, suffix) and verb['pos'][:1] == ['動詞']
                and len(verb['pos']) >= 6 and verb['pos'][5].startswith('連用形')
                and suffix['pos'][:2] == ['接尾辞', '形容詞的']):
            continue
        stem = lemma[:-1]
        end = adjective_inflection_end(
            tokens, i, lemma=lemma, pos='接尾辞', base=lemma,
            past_stem=stem + 'かっ', continuative_stem=stem + 'く',
            conditional_stem=stem + 'けれ')
        if end is None:
            continue
        if next_matches(tokens, end, 'です', '助動詞', 'です'):
            end += 1
        yield suffix['span'][0], tokens[end]['span'][1]


RULES = (RULE,)
