import unittest
from pathlib import Path

from src.SegmentedSentence import SegmentedSentence
from src.TokenProvenance import TokenComponent
from src.WordStats import Sentence
from src.RunConfig import SentencesConfig
from src.steps.AttachSentencesStep import (
    MAX_SENTENCES,
    _percentile,
    _sentence_dedupe_key_fast,
    _sentence_penalty_fast,
    _sentence_quality_penalty_fast,
    attach_sentences,
)

from tests.helpers import make_stats


def seg(text, lemma_surfaces, tag="tag"):
    return SegmentedSentence(
        text=text,
        tag=tag,
        origin=Path("source.txt"),
        lemma_surfaces=lemma_surfaces,
    )


class AttachSentencesStepTests(unittest.TestCase):
    def test_grammar_examples_use_the_same_ranking_as_lexical_examples(self):
        grammar = make_stats(score=700, lemma='〜ている')
        grammar.pos = ('grammar',)
        words = {'〜ている': grammar, '本': make_stats(score=700, lemma='本')}
        short = SegmentedSentence('本を読んでいる。', 'first', Path('first'),
                                  {'〜ている': 'でいる', '本': '本'},
                                  lemma_spans={'〜ている': (4, 7)})
        better = SegmentedSentence('本を楽しく読んでいる。', 'second', Path('second'),
                                   {'〜ている': 'でいる', '本': '本'},
                                   lemma_spans={'〜ている': (7, 10)})
        attach_sentences(words, [short, better], config=SentencesConfig(max_sentences=1, ideal_length=len(better.text)))
        self.assertEqual(grammar.sentences[0].text, better.text)
        self.assertEqual(grammar.sentences[0].origin, Path('second'))
        self.assertEqual(grammar.sentences[0].span, (7, 10))
        self.assertEqual(grammar.sentences[0].score, words['本'].sentences[0].score)

    def test_examples_and_fallback_require_the_selected_occurrence_analysis(self):
        adverb = (TokenComponent("ようやく", "ようやく", "ようやく", "ようやく", ("副詞",)),)
        noun = (TokenComponent("ようやく", "ようやく", "ようやく", "ようやく", ("名詞",)),)
        target = make_stats(score=500, lemma="ようやく")
        target.definition_components = {adverb}
        examples = [SegmentedSentence("ようやく着いた。", "a", Path("a"), {"ようやく": "ようやく"}, lemma_components={"ようやく": adverb}),
                    SegmentedSentence("ようやくを読んだ。", "b", Path("b"), {"ようやく": "ようやく"}, lemma_components={"ようやく": noun})]
        attach_sentences({"ようやく": target}, examples)
        self.assertEqual([s.text for s in target.sentences], ["ようやく着いた。"])

    def test_source_lines_rejected_by_quality_checks_are_available_as_fallback(self):
        from src.steps.TokenizeStep import tokenize
        words, sentences = tokenize(['猫。', '猫が静かに眠っている。'])
        self.assertEqual(words['猫'].frequency, 2)
        attach_sentences(words, sentences)
        self.assertEqual(len(words['猫'].sentences), 2)
        self.assertEqual(words['猫'].sentences[0].text, '猫が静かに眠っている。')
        self.assertEqual(words['猫'].sentences[1].text, '猫。')

    def test_short_source_can_support_a_selected_definition(self):
        from src.steps.TokenizeStep import tokenize
        from src.steps.AddDefinitionsStep import add_and_filter_for_definitions
        words, sentences = tokenize(['できた！'])
        stats = words['できた']
        add_and_filter_for_definitions({'できた': stats}, lambda *args: None, None)
        attach_sentences({'できた': stats}, sentences)
        self.assertIn('good job', stats.definition)
        self.assertEqual([s.text for s in stats.sentences], ['できた！'])
        self.assertFalse(stats.needs_review)

    def test_only_conflicting_source_example_is_attached_for_review(self):
        from src.steps.TokenizeStep import tokenize
        from src.steps.AddDefinitionsStep import add_and_filter_for_definitions
        words, sentences = tokenize(['彼はできた人だ。', 'すぐそこにできた穴から神殿へ行ける。'])
        stats = words['できた']
        add_and_filter_for_definitions({'できた': stats}, lambda *args: None, None)
        conflicting = [s for s in sentences if 'できた穴' in s.text]
        attach_sentences({'できた': stats}, conflicting)
        self.assertEqual([s.text for s in stats.sentences], [conflicting[0].text])
        self.assertTrue(stats.needs_review)
        self.assertTrue(stats.example_review)

    def test_configured_example_limit_and_length_affect_selection(self):
        short = "目標を皆で決めました。"
        long = "目標について皆で詳しく話しました。"
        examples = [seg(short, {"目標": "目標"}), seg(long, {"目標": "目標"})]
        def selected(ideal):
            target = make_stats(score=500, lemma="目標")
            attach_sentences({"目標": target}, examples,
                             config=SentencesConfig(max_sentences=1, ideal_length=ideal))
            self.assertEqual(len(target.sentences), 1)
            return target.sentences[0].text
        self.assertEqual(selected(len(short)), short)
        self.assertEqual(selected(len(long)), long)

    def test_configured_penalties_change_score(self):
        config = SentencesConfig(ideal_length=4, length_divisor=2,
                                 unknown_word_penalty=2, min_word_count=0,
                                 variance_divisor=10)
        self.assertEqual(_sentence_penalty_fast("abcd", [10], 1, 20, config), 3.0)

    def test_percentile_uses_sorted_values_and_caps_index(self):
        self.assertEqual(_percentile([10, 30, 20], 0.9), 30)
        self.assertEqual(_percentile([10, 30, 20], 0.0), 10)

    def test_sentence_penalties_prefer_clean_complete_text(self):
        clean = _sentence_quality_penalty_fast("美鶴は切なげに微笑んだ。")
        edited = _sentence_quality_penalty_fast("美鶴は[...]に微笑んだ。")
        incomplete = _sentence_quality_penalty_fast("美鶴は切なげに微笑んだ")

        self.assertLess(clean, edited)
        self.assertLess(clean, incomplete)
        self.assertGreater(_sentence_penalty_fast("短い。", [100], 1, 0), 0)

    def test_dedupe_key_removes_script_prefix_only_when_body_is_japanese(self):
        self.assertEqual(
            _sentence_dedupe_key_fast("F4 AD 01>美鶴は切なげに微笑んだ。"),
            "美鶴は切なげに微笑んだ。",
        )
        self.assertEqual(_sentence_dedupe_key_fast("plain > text"), "plain > text")

    def test_attach_sentences_keeps_unique_candidates_up_to_the_limit(self):
        target = make_stats(score=500, lemma="目標")
        helper = make_stats(score=100, lemma="簡単")
        word_data = {"目標": target, "簡単": helper}
        sentences = [
            seg("目標と簡単の例文一。", {"目標": "目標", "簡単": "簡単"}, "one"),
            seg("目標と簡単の例文二。", {"目標": "目標", "簡単": "簡単"}, "two"),
            seg("目標と簡単の例文三。", {"目標": "目標", "簡単": "簡単"}, "three"),
            seg("目標と簡単の例文一。", {"目標": "目標", "簡単": "簡単"}, "duplicate"),
        ]

        attach_sentences(word_data, sentences)

        self.assertEqual(len(target.sentences), MAX_SENTENCES)
        texts = [sentence.text for sentence in target.sentences]
        self.assertEqual(len(texts), len(set(texts)))


if __name__ == "__main__":
    unittest.main()
