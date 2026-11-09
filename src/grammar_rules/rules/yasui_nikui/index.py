"""Ease and difficulty expressed by an adjective-like suffix."""
from src.GrammarConstants import YASUI_NIKUI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end, next_matches


RULE = GrammarRule(
    id='yasui_nikui', expression=YASUI_NIKUI_EXPRESSION,
    reading='やすい / にくい', meaning='easy to do / difficult to do',
    match=lambda tokens: match_yasui_nikui(tokens),
)


def match_yasui_nikui(tokens):
    for i in range(1, len(tokens)):
        verb, suffix = tokens[i - 1:i + 1]
        lemma = suffix['lemma']
        if lemma not in ('やすい', 'にくい', '易い', '難い'):
            continue
        if not (adjacent(verb, suffix) and verb['pos'][:1] == ['動詞']
                and len(verb['pos']) >= 6 and verb['pos'][5].startswith('連用形')
                and suffix['pos'][:2] == ['接尾辞', '形容詞的']
                and lemma in ('やすい', 'にくい', '易い', '難い')):
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
