import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch

from src.Artifact import Artifact
from src.Grammar import GrammarStep, GrammarRule, match_te_iru
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
        self.assertIn('grammar', card.tags)

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
        self.assertIn('でいる', plans[0]['add'][0]['fields']['Sentence'])
        self.assertFalse(set(actions) & {'addNotes', 'deleteNotes', 'multi'})

    def test_pipeline_requires_explicit_grammar_flag(self):
        with tempfile.TemporaryDirectory() as temp:
            config = Path(temp) / 'config.toml'
            config.write_text('', encoding='utf-8')
            for enabled in (False, True):
                with self.subTest(enabled=enabled), patch('tango_miner.Pipeline') as pipeline, \
                        patch('tango_miner.collect_dependencies', return_value={}):
                    pipeline.return_value.duration = 0
                    args = ['--config', str(config), '--input', temp]
                    if enabled:
                        args.append('--grammar')
                    process_script(args)
                    steps = pipeline.call_args.kwargs['steps']
                    self.assertEqual(sum(isinstance(s, GrammarStep) for s in steps), int(enabled))
                    from src.steps.WriteOutputStep import WriteOutputStep
                    writer = next(s for s in steps if isinstance(s, WriteOutputStep))
                    self.assertEqual(writer.grammar_step is not None, enabled)

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

    def test_legacy_option_is_an_alias(self):
        with tempfile.TemporaryDirectory() as temp:
            config = Path(temp) / 'config.toml'
            config.write_text('', encoding='utf-8')
            with patch('tango_miner.Pipeline') as pipeline, patch('tango_miner.collect_dependencies', return_value={}):
                pipeline.return_value.duration = 0
                process_script(['--config', str(config), '--input', temp, '--grammar-te-iru'])
                self.assertEqual(sum(isinstance(s, GrammarStep) for s in pipeline.call_args.kwargs['steps']), 1)


if __name__ == '__main__':
    unittest.main()
