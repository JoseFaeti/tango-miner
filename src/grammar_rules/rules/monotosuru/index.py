"""Formal stipulation or assumption: ものとする."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end


def match_monotosuru(tokens):
    for i in range(1, len(tokens) - 2):
        predicate, mono = tokens[i - 1:i + 1]
        if (predicate['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(predicate['pos']) < 6
                or not predicate['pos'][5].startswith('連体形')
                or not adjacent(predicate, mono)
                or mono['surface'] != 'もの' or mono['lemma'] != 'もの'
                or mono['pos'][:1] != ['名詞']
                or not next_matches(tokens, i, 'と', '助詞', 'と')
                or not next_matches(tokens, i + 1, 'する', '動詞')):
            continue
        end = positive_verb_end(tokens, i + 2, lemma='する',
                                base_forms={'する'}, past_stems={'し'},
                                polite_stems={'し'})
        if end is not None:
            yield mono['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='monotosuru', expression='〜ものとする', reading='ものとする',
                   meaning='shall; assume that', match=match_monotosuru)
RULES = (RULE,)
