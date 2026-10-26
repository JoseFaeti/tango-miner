import unittest
from pathlib import Path

from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.TokenizedSentence import TokenizedSentence
from src.steps.BuildWordStatsStep import BuildWordStatsStep
from src.steps.MergeTokensStep import MergeTokensStep
from src.steps.TokenizeStep import RawTokenizeStep


class TokenizerPipelineTests(unittest.TestCase):
    def test_tokenize_step_preserves_raw_token_boundaries(self):
        artifact = Artifact([(Path("fixture.txt"), ["おまもりをください。"])])

        result = Pipeline([RawTokenizeStep()]).run(artifact)

        self.assertEqual([token["surface"] for token in result.data[0].tokens], [
            "お", "まもり", "を", "ください", "。",
        ])
        self.assertEqual(result.data[0].tag, "[no tag]")
        self.assertEqual(result.data[0].origin, Path("fixture.txt"))

    def test_merge_step_merges_without_building_word_stats(self):
        raw_sentence = TokenizedSentence(
            text="おまもりをください。",
            tag="fixture",
            origin=Path("fixture.txt"),
            tokens=[
                {"surface": "お", "lemma": "お", "base_form": "お", "reading": "お", "pos": ["接頭辞", "*", "*", "*"]},
                {"surface": "まもり", "lemma": "まもり", "base_form": "まもり", "reading": "まもり", "pos": ["名詞", "普通名詞", "一般", "*"]},
                {"surface": "を", "lemma": "を", "base_form": "を", "reading": "を", "pos": ["助詞", "格助詞", "*", "*"]},
            ],
        )

        result = Pipeline([MergeTokensStep()]).run(Artifact([raw_sentence]))

        self.assertIsInstance(result.data[0], TokenizedSentence)
        self.assertEqual([token["surface"] for token in result.data[0].tokens], ["おまもり", "を"])

    def test_word_stat_progress_is_batched(self):
        token = {
            "surface": "語", "lemma": "語", "base_form": "語",
            "reading": "ご", "pos": ["名詞", "普通名詞", "一般", "*"],
        }
        tokenized_sentences = [
            TokenizedSentence("語", "fixture", Path("fixture.txt"), [token])
            for _ in range(401)
        ]
        events = []

        Pipeline(
            [BuildWordStatsStep()],
            on_progress=lambda *args, **kwargs: events.append(args),
        ).run(Artifact(tokenized_sentences))

        self.assertEqual([(event[1], event[2]) for event in events[:-1]], [(200, 401), (400, 401)])
        self.assertEqual(events[-1][1:3], (1, 1))
    def test_three_stages_produce_the_existing_word_stats_contract(self):
        artifact = Artifact([(Path("fixture.txt"), ["おまもりをください。\n"])])

        result = Pipeline([
            RawTokenizeStep(),
            MergeTokensStep(),
            BuildWordStatsStep(),
        ]).run(artifact)

        self.assertIn("おまもり", result.data)
        self.assertNotIn("まもり", result.data)
        self.assertEqual(result.data["おまもり"].reading, "おまもり")


if __name__ == "__main__":
    unittest.main()