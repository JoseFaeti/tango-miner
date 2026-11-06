"""Representative predicates followed by たり / だり and する."""
from src.GrammarConstants import TARITARISURU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end


def match_taritarisuru(tokens):
    for i in range(1, len(tokens) - 1):
        predicate, particle = tokens[i - 1:i + 1]
        if particle['surface'] not in ('たり', 'だり'):
            continue
        pos = predicate['pos']
        if (not adjacent(predicate, particle) or len(pos) < 6
                or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not pos[5].startswith('連用形')
                or particle['lemma'] != particle['surface']
                or particle['pos'][:2] != ['助詞', '副助詞']
                or not next_matches(tokens, i, 'する', '動詞')):
            continue
        end = positive_verb_end(tokens, i + 1, lemma='する', base_forms={'する'},
                                past_stems={'し'}, polite_stems={'し'})
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    id='taritarisuru', expression=TARITARISURU_EXPRESSION, reading='たりたりする',
    meaning='do things like; sometimes (representative actions or states)',
    match=match_taritarisuru,
)
