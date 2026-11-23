"""Noun or pronoun + にしたら / にすれば: someone's perspective."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

# Conservative lexical evidence; arbitrary nouns also form literal にする.
_VIEWPOINT_NOUNS = frozenset({
    '人', '者', '方', '子供', '子ども', '大人', '親', '父', '母', '父親', '母親',
    '先生', '教師', '学生', '生徒', '友人', '友達', '客', 'お客', '王女', '王女様',
    'リーダー', '選手', '社員', '社長', '部長', '上司', '部下', '家族', '国民',
    '住民', '市民', '読者', '著者', '筆者', '専門家', '初心者', '会社', '政府',
    '企業', '学校', 'チーム', '側', 'たち', '達', 'ら', '等',
})


def _viewpoint_subject(tokens, ni_index):
    subject = tokens[ni_index - 1]
    pos = subject['pos']
    if not (pos[:1] == ['代名詞']
            or '人名' in pos
            or (pos[:1] == ['名詞'] and subject['lemma'] in _VIEWPOINT_NOUNS)
            or (pos[:1] == ['接尾辞'] and subject['lemma'] in _VIEWPOINT_NOUNS)):
        return False
    # Even a human role can be the result of making somebody something.
    for token in reversed(tokens[:ni_index - 1]):
        if token['pos'][:1] == ['補助記号'] or token['pos'][:1] == ['動詞']:
            break
        if token['surface'] == 'を' and token['pos'][:1] == ['助詞']:
            return False
    return True


def match_nishitara_nisureba(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ni, verb, ending = tokens[i - 1:i + 3]
        if (not _viewpoint_subject(tokens, i)
                or (ni['surface'], ni['lemma'], ni['pos'][:1]) != ('に', 'に', ['助詞'])
                or verb['lemma'] != 'する' or verb['pos'][:1] != ['動詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, ni, verb), (ni, verb, ending)))):
            continue
        if ((verb['surface'], ending['surface'], ending['lemma'], ending['pos'][:1])
                in (('し', 'たら', 'た', ['助動詞']),
                    ('すれ', 'ば', 'ば', ['助詞']))):
            yield ni['span'][0], ending['span'][1]


RULE = GrammarRule(order=12, id='nishitara_nisureba',
                   expression='〜にしたら / 〜にすれば', reading='にしたら / にすれば',
                   meaning='from the perspective of; for', match=match_nishitara_nisureba)
RULES = (RULE,)
