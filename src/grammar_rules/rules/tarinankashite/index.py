"""Tentative examples ending in たりなんかして."""
from src.GrammarConstants import TARINANKASHITE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_tarinankashite(tokens):
    for i in range(1, len(tokens) - 3):
        predicate, particle, nanka, suru, te = tokens[i - 1:i + 4]
        pos = predicate['pos']
        if (particle['surface'] not in ('たり', 'だり')
                or particle['lemma'] != particle['surface']
                or particle['pos'][:2] != ['助詞', '副助詞']
                or len(pos) < 6 or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not pos[5].startswith('連用形')
                or not adjacent(predicate, particle)
                or not next_matches(tokens, i, 'なんか', '助詞', 'なんか')
                or nanka['pos'][:2] != ['助詞', '副助詞']
                or not next_matches(tokens, i + 1, 'する', '動詞', 'し')
                or not next_matches(tokens, i + 2, 'て', '助詞', 'て')):
            continue
        yield particle['span'][0], te['span'][1]


RULE = GrammarRule(id='tarinankashite', expression=TARINANKASHITE_EXPRESSION,
                   reading='たりなんかして',
                   meaning='perhaps do something like; a tentative suggestion or example',
                   match=match_tarinankashite)
