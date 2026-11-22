"""Formal substitution: use one thing in place of another."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end
from src.grammar_rules.rules.omotsute.index import match_omotsute


def match_omotsutekaeru(tokens):
    for start, end in match_omotsute(tokens):
        i = next(i for i, token in enumerate(tokens) if token['span'][1] == end)
        j = i + 1
        # An optional noun phrase names what is being replaced.
        while (j < len(tokens) and adjacent(tokens[j - 1], tokens[j])
               and j - i <= 6 and (tokens[j]['pos'][:1] in (['名詞'], ['代名詞'], ['接頭辞'])
                                    or tokens[j]['surface'] == 'の')):
            j += 1
        if j > i + 1:
            if (j >= len(tokens) or tokens[j]['surface'] != 'に'
                    or tokens[j]['pos'][:1] != ['助詞']
                    or not adjacent(tokens[j - 1], tokens[j])):
                continue
            j += 1
        if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
            continue
        verb = tokens[j]
        last = positive_verb_end(tokens, j, lemma='代える', base_forms=('代える',),
                                 past_stems=('代え',), polite_stems=('代え',),
                                 te_stems=('代え',), conditional_stems=('代えれ',))
        if (last is None and verb['lemma'] == '代える' and verb['surface'] == '代え'
                and verb['pos'][:1] == ['動詞']
                and next_matches(tokens, j, 'させる', '助動詞', 'させ')
                and next_matches(tokens, j + 1, 'て', '助詞', 'て')):
            last = j + 2
        if last is not None:
            yield start, tokens[last]['span'][1]


RULE = GrammarRule(id='omotsutekaeru', expression='〜をもって代える',
                   reading='をもってかえる', meaning='use as a substitute for; serve in place of',
                   match=match_omotsutekaeru, order=-2)
RULES = (RULE,)
