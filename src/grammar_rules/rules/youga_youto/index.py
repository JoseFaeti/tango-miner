"""Concessive volitional verb followed by が or と."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.yougamaiga.index import match_paired_alternatives


def match_youga_youto(tokens):
    pairs = [span for particle in ('が', 'と')
             for span in match_paired_alternatives(tokens, particle)]
    for i, verb in enumerate(tokens):
        if verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6:
            continue
        end = i
        if not verb['pos'][5].startswith('意志推量形'):
            if (not verb['pos'][5].startswith(('連用形', '未然形'))
                    or i + 1 >= len(tokens) or not adjacent(verb, tokens[i + 1])
                    or tokens[i + 1]['surface'] != 'よう'
                    or tokens[i + 1]['pos'][:1] != ['助動詞']):
                continue
            end += 1
        if end + 1 >= len(tokens):
            continue
        particle = tokens[end + 1]
        if (particle['surface'] not in ('が', 'と') or particle['pos'][:1] != ['助詞']
                or not adjacent(tokens[end], particle)):
            continue
        # Sudachi can misread the item after なかれ as a volitional verb in
        # an unpunctuated noun list (なかれやくそうとキメラ...).
        if (particle['surface'] == 'と' and i > 0 and end + 2 < len(tokens)
                and tokens[i - 1]['surface'] == 'なかれ'
                and adjacent(tokens[i - 1], verb)
                and adjacent(particle, tokens[end + 2])
                and tokens[end + 2]['pos'][:1] == ['名詞']):
            continue
        if particle['surface'] == 'と' and end + 2 < len(tokens):
            following = tokens[end + 2]
            if adjacent(particle, following) and following['lemma'] in ('する', '思う', 'おもう'):
                continue
        span = (verb['span'][0], particle['span'][1])
        if not any(a <= span[0] and span[1] <= b for a, b in pairs):
            yield span


RULE = GrammarRule(id='youga_youto', expression='〜ようが / 〜ようと',
                   reading='ようが / ようと', meaning='even if; regardless of whether',
                   match=match_youga_youto)
RULES = (RULE,)
