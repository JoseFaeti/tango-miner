"""Correspondence expressed by に当たる, including ambiguous lexical uses."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, positive_verb_end


def match_niataru(tokens):
    for i in range(1, len(tokens) - 1):
        before, ni, verb = tokens[i - 1:i + 2]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('当たる', 'あたる')
                or not adjacent(before, ni) or not adjacent(ni, verb)):
            continue
        stem = '当た' if verb['lemma'] == '当たる' else 'あた'
        end = positive_verb_end(tokens, i + 1, lemma=verb['lemma'],
                                base_forms=(stem + 'る',), past_stems=(stem + 'っ',),
                                polite_stems=(stem + 'り',), conditional_stems=(stem + 'れ',))
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='niataru', expression='〜にあたる', reading='にあたる',
                   meaning='corresponds to; amounts to', match=match_niataru)
RULES = (RULE,)
