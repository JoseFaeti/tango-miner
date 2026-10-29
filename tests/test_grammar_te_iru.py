import unittest
import csv
from pathlib import Path
import tempfile
from unittest.mock import patch

from src.Artifact import Artifact
from src.Grammar import GrammarStep, GrammarRule, GrammarPositionStep, AddGrammarCardsStep
from src.grammar_rules.rules.te_iru import match_te_iru
from src.GrammarShadow import aligned_tokens
from src.GrammarConstants import TE_IRU_GRAMMAR_EXPRESSION as EXPRESSION
from src.TokenizedSentence import TokenizedSentence
from src.steps.AddWordsToAnkiStep import export_words_to_anki
from tests.helpers import make_stats
from tests.test_add_words_to_anki_step import note
from tango_miner import process_script


def tok(surface, lemma, pos):
    return {'surface': surface, 'lemma': lemma, 'pos': pos}


def sentence(text, tokens):
    return TokenizedSentence(text, 'source', Path('source.txt'), tokens)


class TeIruCardTests(unittest.TestCase):
    def test_observed_inflections_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読んでいる。': 'でいる',
            '本を読んでいた。': 'でいた',
            '本を読んでいない。': 'でいない',
            '本を読んでいなかった。': 'でいなかった',
            '本を読んでいます。': 'でいます',
            '本を読んでいました。': 'でいました',
            '本を読んでいませんでした。': 'でいませんでした',
            '本を読んでる。': 'でる',
            '本を読んでた。': 'でた',
            '本を読んでます。': 'でます',
            '本を見てる。': 'てる',
            '本を読んでない。': 'でない',
            '本を読んでなかった。': 'でなかった',
            '本を見てない。': 'てない',
            '本を見てなかった。': 'てなかった',
            '本を読んでなくて困る。': 'でなく',
            'やってなきゃいいけど。': 'てなきゃ',
            '覚えてなくってさ。': 'てなくって',
            'ハズレてなかったろ？': 'てなかったろ',
            '本を読んでません。': 'でません',
            '本を読んでませんでした。': 'でませんでした',
            '外に出る。': None,
            '彼は学校に出ない。': None,
            'これではない。': None,
            '公園に人がいた。': None,
            '手を洗ってから寝る。': None,
            '見ておいてください。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                found = [text[start:end] for start, end in match_te_iru(aligned)]
                self.assertEqual(found, [] if expected is None else [expected])

    def valid(self):
        return sentence('今日は本を読んでいるよ。', [
            tok('今日は本を', '今日は本を', ('名詞',)), tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            tok('で', 'で', ('助詞', '接続助詞')), tok('いる', 'いる', ('動詞', '非自立可能')),
            tok('よ。', 'よ', ('助詞',))])

    def test_supported_source_builds_separate_card_with_exact_example(self):
        collection = GrammarStep()
        source = self.valid()
        collection.process(Artifact([source, source]))
        words = {'いる': make_stats()}
        collection.add_cards(words)
        card = words[EXPRESSION]
        self.assertEqual(card.frequency, 2)
        self.assertEqual(len(card.sentences), 1)
        self.assertEqual(card.sentences[0].surface_form, 'でいる')
        self.assertIn("<span class='highlight'>でいる</span>", card.sentences[0].to_html())
        self.assertIn('いる', words)
        self.assertEqual(card.tags, {'source'})

    def test_lexical_iru_and_misleading_or_unaligned_cases_do_not_create_card(self):
        items = [sentence('公園に人がいるんですよ。', [tok('公園に人が', '公園', ('名詞',)),
                    tok('いる', 'いる', ('動詞', '非自立可能')), tok('んですよ。', 'ん', ('助詞',))]),
                 sentence('本を読んでいるところ。', [tok('本を読ん', '読む', ('動詞',)),
                    tok('いる', 'いる', ('動詞', '非自立可能')), tok('ところ。', 'ところ', ('名詞',))])]
        collection = GrammarStep()
        collection.process(Artifact(items))
        artifact = Artifact({})
        collection.add_cards(artifact.data)
        self.assertNotIn(EXPRESSION, artifact.data)

    def test_noncontinuative_verb_analysis_does_not_create_card(self):
        source = sentence('ま魔女あんているはずないじゃないですかぁ', [
            tok('ま魔女', '魔女', ('名詞',)),
            tok('あん', 'ある', ('動詞', '非自立可能', '*', '*', '五段-ラ行', '連体形-撥音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能')),
            tok('はずないじゃないですかぁ', 'はずない', ('形容詞',))])
        collection = GrammarStep()
        collection.process(Artifact([source]))
        self.assertEqual(collection.occurrences['te_iru'].count, 0)

    def test_incomplete_contracted_negative_does_not_create_partial_match(self):
        source = sentence('本を読んでなかっ', [
            tok('本を', '本', ('名詞',)),
            tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            tok('で', 'で', ('助詞', '接続助詞')),
            tok('なかっ', 'ない', ('形容詞', '非自立可能'))])
        aligned, error = aligned_tokens(source.text, source.tokens)
        self.assertIsNone(error)
        self.assertEqual(list(match_te_iru(aligned)), [])

    def test_existing_grammar_note_is_neither_overwritten_nor_pruned(self):
        existing = note(123, EXPRESSION, fields={'Meaning': 'user edited'})
        for words in ({}, {EXPRESSION: make_stats()}):
            actions, plans = [], []
            def invoke(action, params=None):
                actions.append(action)
                if action == 'modelNames': return ['Custom']
                if action == 'findNotes': return [123]
                if action == 'notesInfo': return [existing]
                raise AssertionError(action)
            export_words_to_anki('Deck', words, 'Custom', invoke=invoke,
                                 prune=True, output_only=True, details_handler=plans.append)
            self.assertEqual(plans[0]['modify'], [])
            self.assertEqual(plans[0]['delete'], [])
            self.assertEqual(plans[0]['retained_existing_grammar'][0]['note_id'], 123)
            self.assertFalse(set(actions) & {'addNotes', 'deleteNotes', 'multi'})

    def test_existing_note_only_removes_synthetic_grammar_tag(self):
        for source_tags, expected in [({'source'}, ['grammar']), ({'grammar'}, [])]:
            with self.subTest(source_tags=source_tags):
                existing = note(123, EXPRESSION, tags=['grammar', 'custom'],
                                fields={'Meaning': 'user edit'})
                plans = []
                def invoke(action, params=None):
                    if action == 'modelNames': return ['Custom']
                    if action == 'findNotes': return [123]
                    if action == 'notesInfo': return [existing]
                    raise AssertionError(action)
                export_words_to_anki('Deck', {EXPRESSION: make_stats(tags=source_tags)},
                                     'Custom', invoke=invoke, output_only=True,
                                     details_handler=plans.append)
                modifications = plans[0]['modify']
                self.assertEqual(len(modifications), bool(expected))
                if expected:
                    self.assertEqual(modifications[0]['tags_removed'], expected)
                    self.assertEqual(modifications[0]['tags_added'], [])
                    self.assertEqual(modifications[0]['fields'], {})

    def test_new_note_plan_contains_the_grammar_card_without_mutations(self):
        collection = GrammarStep()
        collection.process(Artifact([self.valid()]))
        artifact = Artifact({})
        collection.add_cards(artifact.data)
        actions, plans = [], []
        def invoke(action, params=None):
            actions.append(action)
            if action == 'modelNames': return ['Custom']
            if action == 'findNotes': return []
            raise AssertionError(action)
        export_words_to_anki('Deck', artifact.data, 'Custom', invoke=invoke,
                             output_only=True, details_handler=plans.append)
        self.assertEqual(plans[0]['add'][0]['expression'], EXPRESSION)
        self.assertEqual(plans[0]['add'][0]['tags'], ['source'])
        self.assertIn('でいる', plans[0]['add'][0]['fields']['Sentence'])
        self.assertFalse(set(actions) & {'addNotes', 'deleteNotes', 'multi'})

    def test_pipeline_enables_grammar_by_default_and_allows_opt_out(self):
        with tempfile.TemporaryDirectory() as temp:
            config = Path(temp) / 'config.toml'
            config.write_text('', encoding='utf-8')
            for option, enabled in ((None, True), ('--no-grammar', False)):
                with self.subTest(option=option), patch('tango_miner.Pipeline') as pipeline, \
                        patch('tango_miner.collect_dependencies', return_value={}):
                    pipeline.return_value.duration = 0
                    args = ['--config', str(config), '--input', temp]
                    if option:
                        args.append(option)
                    process_script(args)
                    steps = pipeline.call_args.kwargs['steps']
                    self.assertEqual(sum(isinstance(s, GrammarStep) for s in steps), int(enabled))
                    from src.steps.ScoreWordStep import ScoreWordStep
                    self.assertEqual(sum(isinstance(s, GrammarPositionStep) for s in steps), int(enabled))
                    self.assertEqual(sum(isinstance(s, AddGrammarCardsStep) for s in steps), int(enabled))
                    if enabled:
                        self.assertLess(next(i for i, s in enumerate(steps) if isinstance(s, AddGrammarCardsStep)),
                                        next(i for i, s in enumerate(steps) if isinstance(s, ScoreWordStep)))

    def test_grammar_uses_merged_positions_and_same_scoring_as_vocabulary(self):
        from src.steps.ScoreWordStep import score_words
        def token(surface, lemma, pos):
            return {'surface': surface, 'lemma': lemma, 'base_form': lemma,
                    'reading': lemma, 'pos': pos}
        noun = ('名詞', '普通名詞')
        particle = ('助詞', '係助詞')
        source = sentence('今日は本を読んでいるよ。', [
            token('今日', '今日', noun), token('は', 'は', particle),
            token('本', '本', noun), token('を', 'を', particle),
            token('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            token('で', 'で', ('助詞', '接続助詞')),
            token('いる', 'いる', ('動詞', '非自立可能')),
            token('よ', 'よ', particle), token('。', '。', ('補助記号', '句点'))])
        grammar = GrammarStep()
        raw = Artifact([source])
        grammar.process(raw)
        GrammarPositionStep(grammar).process(raw)
        words = {}
        AddGrammarCardsStep(grammar).process(Artifact(words))
        card = words[EXPRESSION]
        self.assertEqual(card.index_count, 1)
        self.assertEqual(card.index, 0.75)
        lexical = make_stats()
        lexical.frequency = card.frequency
        lexical.index = card.index
        lexical.tags = {'source'}
        words['lexical'] = lexical
        score_words(words)
        self.assertGreater(card.score, 0)
        self.assertEqual(card.score, lexical.score)
        self.assertEqual(card.sentences[0].surface_form, 'でいる')
        from src.steps.WriteOutputStep import WriteOutputStep
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'output.csv'
            WriteOutputStep(path).process(Artifact(words))
            with path.open(encoding='utf-8', newline='') as stream:
                row = next(row for row in csv.reader(stream) if row[1] == EXPRESSION)
            self.assertEqual(float(row[0]), card.score)
            self.assertGreater(float(row[0]), 0)

    def test_multiple_rules_share_one_stage_and_detect_collisions(self):
        other = GrammarRule('second', '〜別 (grammar)', 'べつ', 'second rule',
                            lambda tokens: [(0, tokens[0]['span'][1])])
        collection = GrammarStep(rules=(GrammarRule('te_iru', EXPRESSION, 'ている', 'meaning', match_te_iru), other))
        collection.process(Artifact([self.valid()]))
        words = {}
        collection.add_cards(words)
        self.assertEqual(set(words), {EXPRESSION, '〜別 (grammar)'})
        with self.assertRaises(ValueError):
            collection.add_cards(words)

    def test_removed_enable_options_are_rejected(self):
        for option in ('--grammar', '--grammar-te-iru'):
            with self.subTest(option=option), self.assertRaises(SystemExit) as error:
                process_script([option])
            self.assertEqual(error.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
