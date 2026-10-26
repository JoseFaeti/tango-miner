import unittest
from pathlib import Path

from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.steps.TokenizeStep import TokenizeStep, tokenize


class TokenizeStepTests(unittest.TestCase):
    def test_tokenize_collects_lemmas_and_a_segmented_sentence(self):
        words, sentences = tokenize(["彼はすでに家に帰っている。"], tag="fixture")

        self.assertEqual(list(words), ["すでに", "家", "帰る", "いる"])
        self.assertEqual(words["帰る"].frequency, 1)
        self.assertEqual(words["家"].tags, {"fixture"})
        self.assertEqual(sentences[0].text, "彼はすでに家に帰っている。")
        self.assertEqual(sentences[0].lemma_surfaces["帰る"], "帰っ")

    def test_step_accepts_sentences_grouped_by_source_file(self):
        artifact = Artifact(
            [(Path("fixture.txt"), ["彼はすでに家に帰っている."])],
        )

        result = Pipeline([TokenizeStep()]).run(artifact)

        self.assertIn("帰る", result.data)
        self.assertEqual(len(result.sentences), 1)

    def test_keeps_kana_only_candidates_from_suspended_examples(self):
        """Ambiguous kana remains available for recall-first mining."""
        words, _ = tokenize([
            "何をしようが豪快にプチ・ブレスを吐いて勝利となります。",
            "さあ、さいだんのマナの種子に聖剣をかかげてみなさい。",
        ])

        self.assertIn("しよう", words)
        self.assertIn("さい", words)

    def test_keeps_kana_only_auxiliary_from_suspended_example(self):
        """Grammar forms remain available for recall-first mining."""
        words, _ = tokenize(["お前の後についてくから適当にがんばってくれ。"])

        self.assertIn("てく", words)

    def test_keeps_lexical_kana_adverb_from_suspended_examples(self):
        """The gate must not discard useful kana-only lexical vocabulary."""
        words, _ = tokenize(["長年探し続けて、ようやく見つけることができたわい。"])

        self.assertIn("ようやく", words)

    def test_keeps_long_kana_nouns(self):
        """Kana-only source text can contain legitimate lexical nouns."""
        words, _ = tokenize(["楽しいおもいでができた。"])

        self.assertIn("おもいで", words)


if __name__ == "__main__":
    unittest.main()
