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


if __name__ == "__main__":
    unittest.main()
