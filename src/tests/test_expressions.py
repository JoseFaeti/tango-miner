import unittest
import csv
import json
import tempfile
from pathlib import Path

from src.Expressions import find_expressions, inventory, validate_inventory
from src.TokenizedSentence import TokenizedSentence
from src.steps.TokenizeStep.index import tokenize_raw_sentences
from src.Artifact import Artifact
from src.Expressions import ExpressionStep, AddExpressionCandidatesStep
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.rules.te_iru.index import RULE as TE_IRU
from src.grammar_rules.rules.te_shimau.index import RULE as TE_SHIMAU
from src.grammar_rules.rules.sou_da.index import HEARSAY
from src.steps.BuildWordStatsStep.index import BuildWordStatsStep
from src.steps.AddDefinitionsStep.index import AddDefinitionsStep
from src.steps.FilterFrequencyStep.index import FilterFrequencyStep
from src.steps.MergeTokensStep.index import MergeTokensStep
from src.steps.MergeTokensStep.index import get_jmdict
from src.steps.AttachSentencesStep.index import attach_sentences
from src.steps.ScoreWordStep.index import score_words
from src.steps.WriteOutputStep.index import write_final_file, write_candidate_diagnostics


class ExpressionMatchingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entries = inventory()

    def match(self, *texts):
        sentences = tokenize_raw_sentences(list(texts), tag='test', source_path=Path('test.txt'))
        return [[(m.entry['expression'], sentence.text[m.start:m.end], m.omitted_particle,
                  ''.join(c.surface for c in m.components))
                 for m in find_expressions(sentence, self.entries)] for sentence in sentences]

    def test_all_catalogue_headwords_are_detected(self):
        for entry in self.entries:
            with self.subTest(expression=entry['expression']):
                matches = self.match(entry['expression'] + '。')[0]
                self.assertEqual([m[0] for m in matches], [entry['expression']])

    def test_inventory_is_missing_from_tested_dictionary_in_writing_and_reading(self):
        dictionary = get_jmdict()
        for entry in self.entries:
            with self.subTest(expression=entry['expression']):
                self.assertFalse(dictionary.lookup_word(entry['expression']))
                self.assertFalse(dictionary.lookup_word(entry['reading']))

    def test_inventory_rejects_malformed_and_duplicate_entries(self):
        valid = dict(self.entries[0])
        for data in ({'version': 2, 'entries': [valid]},
                     {'version': 1, 'entries': [valid, valid]},
                     {'version': 1, 'entries': [{**valid, 'noun': '幕'}]},
                     {'version': 1, 'entries': [{**valid, 'verb_class': 'unknown'}]},
                     {'version': 1, 'entries': [{**valid, 'verb': ['書く'], 'verb_class': 'ichidan'}]}):
            with self.subTest(data=data), self.assertRaises(ValueError):
                validate_inventory(data)

    def test_inflection_variants_and_dropped_particles(self):
        texts = ['青筋を立てる。', '青筋を立てた。', '青筋を立てない。',
                 '青筋立てられた。', '青筋を立てさせる。',
                 '唇を噛んだ。', '胸が膨らんでいる。',
                 'らっぱを吹ける。', 'らっぱ吹かされた。']
        expected = ['青筋を立てる', '青筋を立てる', '青筋を立てる',
                    '青筋を立てる', '青筋を立てる', '唇をかむ',
                    '胸が膨らむ', 'らっぱを吹く', 'らっぱを吹く']
        results = self.match(*texts)
        for text, word, matches in zip(texts, expected, results):
            with self.subTest(text=text):
                self.assertEqual([m[0] for m in matches], [word])
                self.assertEqual(matches[0][1], matches[0][3])
        self.assertTrue(results[3][0][2])
        self.assertTrue(results[8][0][2])
        self.assertEqual(results[1][0][1], '青筋を立てた')
        self.assertEqual(results[2][0][1], '青筋を立てない')

    def test_common_inflection_and_contraction_matrix(self):
        cases = {
            '青筋を立てます。': '青筋を立てます',
            '青筋を立てませんでした。': '青筋を立てませんでした',
            '青筋を立てれば。': '青筋を立てれば',
            '青筋を立てよう。': '青筋を立てよう',
            '青筋を立てろ。': '青筋を立てろ',
            '青筋を立てられる。': '青筋を立てられる',
            '青筋を立てれる。': '青筋を立てれる',
            '青筋を立てちゃった。': '青筋を立てちゃった',
            '青筋を立ててしまった。': '青筋を立てて',
            '青筋を立てず。': '青筋を立てず',
            '青筋を立てられません。': '青筋を立てられません',
            'らっぱを吹かない。': 'らっぱを吹かない',
            'らっぱを吹こう。': 'らっぱを吹こう',
            'らっぱを吹けない。': 'らっぱを吹けない',
            'らっぱを吹かせる。': 'らっぱを吹かせる',
            'らっぱを吹かれた。': 'らっぱを吹かれた',
            'らっぱを吹いている。': 'らっぱを吹いて',
            '唇をかめば。': '唇をかめば',
            '胸が膨らもう。': '胸が膨らもう',
        }
        for (text, surface), matches in zip(cases.items(), self.match(*cases)):
            with self.subTest(text=text):
                self.assertEqual([m[1] for m in matches], [surface])

    def test_each_inventory_verb_across_inflections_and_particle_omission(self):
        rows = {'く': ('か', 'き', 'け', 'こ', 'いた', 'いて'),
                'む': ('ま', 'み', 'め', 'も', 'んだ', 'んで')}
        cases = []
        for entry in self.entries:
            if entry['verb_class'] == 'i_adjective':
                continue
            verb = entry['verb'][0]
            stem = verb[:-1]
            if entry['verb_class'] == 'ichidan':
                forms = [verb, stem + 'ます', stem + 'ない', stem + 'た', stem + 'れば',
                         stem + 'よう', stem + 'ろ', stem + 'られる', stem + 'させる', stem + 'れる',
                         stem + 'たい', stem + 'なければ', stem + 'なかったら', stem + 'させられる',
                         stem + 'られなければ', stem + 'させられなかった', stem + 'たくなかった',
                         stem + 'ながら', stem + 'てしまわなかった', stem + 'ていられない',
                         stem + 'させられたくない']
            else:
                a, i, e, o, past, te = rows[verb[-1]]
                forms = [verb, stem + i + 'ます', stem + a + 'ない', stem + past,
                         stem + e + 'ば', stem + o + 'う', stem + e,
                         stem + a + 'れる', stem + a + 'せる', stem + e + 'る',
                         stem + i + 'たい', stem + a + 'なければ',
                         stem + a + 'なかったら', stem + a + 'せられる',
                         stem + a + 'れなければ', stem + a + 'せられなかった',
                         stem + i + 'たくなかった', stem + i + 'ながら',
                         stem + te + 'しまわなかった', stem + te + 'いられない',
                         stem + a + 'せられたくない']
            for particle in (entry['particle'], ''):
                for form in forms:
                    cases.append((entry['noun'][0] + particle + form + '。', entry['expression']))
        results = self.match(*(text for text, _ in cases))
        for (text, expected), matches in zip(cases, results):
            with self.subTest(text=text):
                self.assertIn(expected, [m[0] for m in matches])

    def test_adjective_inflections_with_and_without_particle(self):
        for entry in self.entries:
            if entry['verb_class'] != 'i_adjective':
                continue
            stem = entry['verb'][0][:-1]
            forms = [stem + ending for ending in ('い', 'かった', 'くない', 'くなかった',
                     'ければ', 'くて', 'くありません', 'くありませんでした')]
            cases = [(entry['noun'][0] + particle + form + '。', entry['expression'])
                     for particle in (entry['particle'], '') for form in forms]
            for (text, expected), matches in zip(cases, self.match(*(text for text, _ in cases))):
                with self.subTest(text=text):
                    self.assertIn(expected, [m[0] for m in matches])
                    self.assertEqual(matches[0][1], text[:-1])
                    self.assertEqual(matches[0][1], matches[0][3])

    def test_adjective_does_not_match_unrelated_predicate_or_particle(self):
        for matches in self.match('舌を長い。', '舌が短い。', '胸が冷たい。'):
            self.assertFalse(any(m[0] in {'舌が長い', '胸が熱い'} for m in matches))

    def test_adjective_expression_coexists_with_hearsay_grammar(self):
        raw = tokenize_raw_sentences(['彼の話では胸が熱いそうです。'],
                                     tag='source', source_path=Path('test.txt'))
        artifact = Artifact(raw)
        expression = ExpressionStep()
        expression.process(artifact)
        grammar = GrammarStep(rules=(HEARSAY,))
        grammar.process(artifact)
        self.assertEqual(grammar.occurrences[HEARSAY.id].count, 1)
        self.assertEqual([m.entry['expression'] for m in expression.matches[0]], ['胸が熱い'])

    def test_adjective_candidate_keeps_definition_examples_and_components(self):
        raw = tokenize_raw_sentences(['彼女は胸が熱くなかったそうです。', 'その話で胸熱かった。'],
                                     tag='source', source_path=Path('test.txt'))
        artifact = Artifact(raw)
        expression = ExpressionStep()
        expression.process(artifact)
        build = BuildWordStatsStep()
        build.start_time = None
        artifact = AddExpressionCandidatesStep(expression).process(build.process(artifact))
        definitions = AddDefinitionsStep()
        definitions.start_time = None
        artifact = definitions.process(artifact)
        stats = artifact.data['胸が熱い']
        self.assertEqual(stats.frequency, 2)
        self.assertEqual(stats.definition, 'to be overcome with emotion')
        self.assertTrue(stats.needs_review)
        self.assertEqual(sum(stats.component_counts.values()), 2)
        self.assertEqual(artifact.sentences[0].lemma_surfaces['胸が熱い'], '胸が熱くなかった')

    def test_contracted_omitted_particle_forms(self):
        cases = [('幕開けちゃった。', '幕を開ける'), ('幕開けてる。', '幕を開ける'),
                 ('唇かんじゃった。', '唇をかむ'), ('らっぱ吹いといた。', 'らっぱを吹く')]
        for (text, expected), matches in zip(cases, self.match(*(text for text, _ in cases))):
            with self.subTest(text=text):
                self.assertIn(expected, [m[0] for m in matches])

    def test_merged_continuative_stems_remain_review_candidates(self):
        cases = [('幕開け、話が始まった。', '幕を開ける'),
                 ('筆入れ、文章を直した。', '筆を入れる')]
        for (text, expected), matches in zip(cases, self.match(*(text for text, _ in cases))):
            with self.subTest(text=text):
                self.assertIn(expected, [m[0] for m in matches])
        source = tokenize_raw_sentences(['筆入れを買いました。'], tag='source', source_path=Path('test'))
        artifact = Artifact(source)
        expression = ExpressionStep()
        expression.process(artifact)
        build = BuildWordStatsStep()
        build.start_time = None
        artifact = AddExpressionCandidatesStep(expression).process(build.process(artifact))
        self.assertIn('筆入れ', artifact.data)
        self.assertTrue(artifact.data['筆を入れる'].expression_review)

    def test_long_auxiliary_chains_after_merged_nouns(self):
        cases = [('幕開けさせられませんでした。', '幕を開ける'),
                 ('筆入れられなかったら。', '筆を入れる'),
                 ('手広げさせられない。', '手を広げる'),
                 ('唇かませられなかった。', '唇をかむ')]
        sentences = tokenize_raw_sentences([text for text, _ in cases], tag='test', source_path=Path('test'))
        for sentence, (_, expected) in zip(sentences, cases):
            with self.subTest(text=sentence.text):
                matches = find_expressions(sentence)
                self.assertIn(expected, [match.entry['expression'] for match in matches])
                for match in matches:
                    self.assertEqual(sentence.text[match.start:match.end],
                                     ''.join(component.surface for component in match.components))

    def test_reanalysis_extends_merged_stem_through_auxiliary_chain(self):
        text = '幕開けさせられませんでした。'
        sentence = tokenize_raw_sentences([text], tag='source', source_path=Path('test'))[0]
        matches = [m for m in find_expressions(sentence) if m.entry['expression'] == '幕を開ける']
        self.assertEqual(len(matches), 1)
        match = matches[0]
        self.assertEqual(text[match.start:match.end], '幕開けさせられませんでした')
        self.assertEqual(match.analysis_rule, 'verb_reanalysis')
        self.assertTrue(match.omitted_particle)
        self.assertEqual(''.join(c.surface for c in match.components), text[:-1])

    def test_different_particles_words_and_broken_alignment_do_not_match(self):
        for matches in self.match('青筋に立てる。', '青筋を探す。', '青筋、立てる。',
                                  '手を上げる。', '青筋 を立てる。'):
            self.assertEqual(matches, [])
        source = tokenize_raw_sentences(['青筋を立てる。'], tag='test', source_path=Path('test'))[0]
        broken = TokenizedSentence(source.text, source.tag, source.origin, source.tokens[:-1])
        self.assertEqual(find_expressions(broken), [])

    def test_recovery_keeps_merged_analyzer_evidence_in_diagnostics(self):
        raw = tokenize_raw_sentences(['幕開けた。', '幕を開けた。'], tag='source', source_path=Path('test.txt'))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'expressions.jsonl'
            ExpressionStep(path).process(Artifact(raw))
            with path.open(encoding='utf-8') as stream:
                rows = [json.loads(line) for line in stream]
        self.assertEqual([row['candidate'] for row in rows], ['幕を開ける', '幕を開ける'])
        self.assertEqual([row['surface'] for row in rows], ['幕開けた', '幕を開けた'])
        self.assertEqual([row['omitted_particle'] for row in rows], [True, False])
        self.assertEqual([row['analysis_rule'] for row in rows], ['surface_recovery', 'sudachi'])
        self.assertEqual(''.join(part['surface'] for part in rows[0]['components']), '幕開けた')
        self.assertEqual(rows[0]['span'], [0, 4])

    def test_repeated_matches_are_in_source_order(self):
        matches = self.match('幕開けた。幕を開けた。')[0]
        self.assertEqual([m[1] for m in matches], ['幕開けた', '幕を開けた'])

    def test_existing_lexical_occurrence_is_not_double_counted(self):
        raw = tokenize_raw_sentences(['青筋を立てる。', '青筋立てた。'], tag='source', source_path=Path('test.txt'))
        first = raw[0]
        combined = {'surface': '青筋を立てる', 'lemma': '青筋を立てる',
                    'base_form': '青筋を立てる', 'reading': 'あおすじをたてる',
                    'pos': ('名詞', '普通名詞', '一般', '*', '*', '*')}
        raw[0] = TokenizedSentence(first.text, first.tag, first.origin, [combined, first.tokens[-1]])
        second = raw[1]
        raw[1] = TokenizedSentence(second.text, second.tag, Path('other.txt'), second.tokens)
        artifact = Artifact(raw)
        expressions = ExpressionStep()
        expressions.process(artifact)
        build = BuildWordStatsStep()
        build.start_time = None
        artifact = build.process(artifact)
        self.assertEqual(artifact.data['青筋を立てる'].frequency, 1)
        artifact = AddExpressionCandidatesStep(expressions).process(artifact)
        self.assertEqual(artifact.data['青筋を立てる'].frequency, 2)
        self.assertEqual(artifact.data['青筋を立てる'].index_count, 2)
        self.assertEqual(len(artifact.sentences[0].lemma_occurrences['青筋を立てる']), 1)
        self.assertEqual(artifact.sentences[1].lemma_surfaces['青筋を立てる'], '青筋立てた')

    def test_completion_grammar_is_not_suppressed_by_expression(self):
        raw = tokenize_raw_sentences(['彼は青筋立ててしまったそうです。'],
                                     tag='source', source_path=Path('test.txt'))
        expression = ExpressionStep()
        artifact = expression.process(Artifact(raw))
        grammar = GrammarStep(rules=(TE_SHIMAU,), prefer_expressions=True)
        grammar.process(artifact)
        self.assertEqual(grammar.occurrences[TE_SHIMAU.id].count, 1)
        self.assertEqual([match.entry['expression'] for match in expression.matches[0]], ['青筋を立てる'])
        ExcludeGrammarTokensStep(grammar).process(artifact)
        self.assertTrue(any(token.get('grammar_reserved') for token in artifact.data[0].tokens))

    def test_expression_and_grammar_and_components_survive_together(self):
        texts = ['彼は青筋を立てているそうです。', '彼女は青筋立てたそうです。']
        raw = tokenize_raw_sentences(texts, tag='source', source_path=Path('test.txt'))
        artifact = Artifact(raw)
        expression_stage = ExpressionStep()
        expression_stage.process(artifact)
        grammar = GrammarStep(rules=(TE_IRU,))
        grammar.process(artifact)
        self.assertEqual(grammar.occurrences[TE_IRU.id].count, 1)
        ExcludeGrammarTokensStep(grammar).process(artifact)
        merge = MergeTokensStep()
        merge.start_time = None
        artifact = merge.process(artifact)
        build = BuildWordStatsStep()
        build.start_time = None
        artifact = build.process(artifact)
        artifact = AddExpressionCandidatesStep(expression_stage).process(artifact)
        stats = artifact.data['青筋を立てる']
        self.assertEqual(stats.frequency, 2)
        self.assertIn('青筋', artifact.data)
        self.assertEqual(sum(stats.component_counts.values()), 2)
        self.assertEqual(artifact.sentences[0].lemma_surfaces['青筋を立てる'], '青筋を立てて')
        self.assertEqual(artifact.sentences[1].lemma_surfaces['青筋を立てる'], '青筋立てた')
        filtering = FilterFrequencyStep(1, keep_percent=100, min_frequency_per_source=0)
        filtering.start_time = None
        artifact = filtering.process(artifact)
        definitions = AddDefinitionsStep()
        definitions.start_time = None
        artifact = definitions.process(artifact)
        self.assertEqual(artifact.data['青筋を立てる'].definition, 'to become visibly furious')
        self.assertTrue(artifact.data['青筋を立てる'].needs_review)
        grammar.add_cards(artifact.data)
        self.assertIn(TE_IRU.expression, artifact.data)
        score_words(artifact.data)
        attach_sentences(artifact.data, artifact.sentences)
        stats = artifact.data['青筋を立てる']
        self.assertEqual({s.surface_form for s in stats.sentences}, {'青筋を立てて', '青筋立てた'})
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'output.csv'
            diagnostics = Path(directory) / 'diagnostics.jsonl'
            write_final_file(artifact.data, output)
            write_candidate_diagnostics(artifact.data, diagnostics)
            with output.with_name('output.dropped.csv').open(encoding='utf-8', newline='') as stream:
                self.assertIn('青筋を立てる', [row[1] for row in csv.reader(stream)])
            with diagnostics.open(encoding='utf-8') as stream:
                rows = [json.loads(line) for line in stream]
            row = next(row for row in rows if row['candidate'] == '青筋を立てる')
            self.assertIn('expression_may_be_literal', row['review_reasons'])


if __name__ == '__main__':
    unittest.main()
