"""Noteworthy qualities expressed with a predicate + ものがある."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end
from src.grammar_rules.rules.toiumonogaaru.index import match_toiumonogaaru


def match_mono_ga_aru(tokens):
    longer_spans = list(match_toiumonogaaru(tokens))
    for i in range(1, len(tokens) - 2):
        modifier, mono = tokens[i - 1:i + 1]
        if (mono['surface'] != 'もの' or mono['pos'][:1] != ['名詞']
                or not adjacent(modifier, mono)):
            continue
        pos = modifier['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith('連体形')):
            continue
        if not next_matches(tokens, i, 'が', '助詞', 'が'):
            continue
        if not adjacent(tokens[i + 1], tokens[i + 2]):
            continue
        end = positive_verb_end(tokens, i + 2, lemma='ある',
                                base_forms={'ある'}, past_stems={'あっ'},
                                polite_stems={'あり'})
        if end is not None and not any(start <= mono['span'][0] < stop for start, stop in longer_spans):
            yield mono['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    order=153, id='mono_ga_aru', expression='〜ものがある', reading='ものがある',
    meaning='have a noteworthy quality; there is something that evokes a feeling',
    match=match_mono_ga_aru,
)
RULES = (RULE,)
