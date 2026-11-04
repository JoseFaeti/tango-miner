import unittest
import tempfile
from unittest.mock import Mock, patch
from pathlib import Path

from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.TokenizedSentence import TokenizedSentence
from src.TokenCache import TokenCache
from src.RunConfig import TokenizerConfig
from src.steps.BuildWordStatsStep import BuildWordStatsStep
from src.steps.MergeTokensStep import MergeTokensStep
from src.steps.TokenizeStep import (RawTokenizeStep, tokenize_raw_sentences, tokenizer_fingerprint,
                                    tokenizer_source_fingerprint, TOKENIZER_SOURCE_DEPENDENCIES, get_tokenizer)


class TokenizerPipelineTests(unittest.TestCase):
    def test_source_fingerprint_tracks_dependencies_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in TOKENIZER_SOURCE_DEPENDENCIES:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("original", encoding="utf-8")
            unrelated = root / "other.py"
            unrelated.write_text("original", encoding="utf-8")
            first = tokenizer_source_fingerprint(root)
            unrelated.write_text("changed", encoding="utf-8")
            self.assertEqual(first, tokenizer_source_fingerprint(root))
            for name in TOKENIZER_SOURCE_DEPENDENCIES:
                path = root / name
                path.write_text("changed", encoding="utf-8")
                self.assertNotEqual(first, tokenizer_source_fingerprint(root), name)
                path.write_text("original", encoding="utf-8")

    def test_source_change_invalidates_token_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.txt"
            source.write_text("猫", encoding="utf-8")
            config = root / "config.json"
            config.write_text("{}", encoding="utf-8")
            dictionary = root / "system.dic"
            dictionary.write_bytes(b"dictionary")
            settings = TokenizerConfig(config_path=str(config), dictionary=str(dictionary))
            token = {"surface": "猫", "lemma": "猫", "base_form": "猫", "reading": "ねこ", "pos": ("名詞",)}
            tokenizer = Mock()
            tokenizer.tokenize.return_value = [object()]
            with patch("src.steps.TokenizeStep.tokenizer_source_fingerprint", side_effect=["first", "first", "changed"]), \
                 patch("src.steps.TokenizeStep.appdirs.user_cache_dir", return_value=tmp), \
                 patch("src.steps.TokenizeStep.get_tokenizer", return_value=tokenizer), \
                 patch("src.steps.TokenizeStep.sudachi_node_to_dict", return_value=token):
                for _ in range(3):
                    Pipeline([RawTokenizeStep(config=settings)]).run(Artifact([(source, ["猫"])], tmpdir=root))
            self.assertEqual(tokenizer.tokenize.call_count, 2)

    def test_fingerprint_changes_with_settings_and_json_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sudachi.json"
            path.write_text("{}", encoding="utf-8")
            dictionary_path = Path(tmp) / "system.dic"
            dictionary_path.write_bytes(b"first")
            full = TokenizerConfig(config_path=str(path), dictionary=str(dictionary_path))
            small = TokenizerConfig(config_path=str(path), dictionary=str(dictionary_path))
            first = tokenizer_fingerprint(full)
            self.assertEqual(first, tokenizer_fingerprint(full))
            dictionary_path.write_bytes(b"other")
            self.assertNotEqual(first, tokenizer_fingerprint(small))
            path.write_text('{"test": true}', encoding="utf-8")
            self.assertNotEqual(first, tokenizer_fingerprint(full))

    def test_fingerprint_tracks_user_dictionary_contents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            system = root / "system.dic"
            user = root / "user.dic"
            config = root / "config.json"
            system.write_bytes(b"system")
            user.write_bytes(b"first")
            config.write_text(__import__("json").dumps({"userDict": [str(user)]}), encoding="utf-8")
            settings = TokenizerConfig(config_path=str(config), dictionary=str(system))
            before = tokenizer_fingerprint(settings)
            user.write_bytes(b"other")
            self.assertNotEqual(before, tokenizer_fingerprint(settings))

    def test_changed_sentences_with_same_timestamp_are_retokenized(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source.txt"
            source.write_text("猫", encoding="utf-8")
            cache = TokenCache(Path(tmp) / "cache", "version")
            token = {"surface": "猫", "lemma": "猫", "base_form": "猫", "reading": "ねこ", "pos": ("名詞",)}
            tokenizer = Mock()
            tokenizer.tokenize.return_value = [object()]
            with patch("src.steps.TokenizeStep.get_tokenizer", return_value=tokenizer), \
                 patch("src.steps.TokenizeStep.sudachi_node_to_dict", return_value=token):
                tokenize_raw_sentences(["猫"], tag="a", source_path=source, cache=cache)
                result = tokenize_raw_sentences(["犬", "猫"], tag="b", source_path=source, cache=cache)
                cached = tokenize_raw_sentences(["犬", "猫"], tag="c", source_path=source, cache=cache)
            self.assertEqual(tokenizer.tokenize.call_count, 3)
            self.assertEqual([s.text for s in result], ["犬", "猫"])
            self.assertEqual([s.tag for s in cached], ["c", "c"])
            self.assertEqual([s.tokens for s in result], [s.tokens for s in cached])

    def test_tokenizer_reloads_when_settings_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sudachi.json"
            path.write_text("{}", encoding="utf-8")
            first_config = TokenizerConfig(config_path=str(path), dictionary="full")
            second_config = TokenizerConfig(config_path=str(path), dictionary="small")
            with patch("src.steps.TokenizeStep._tokenizer", None), \
                 patch("src.steps.TokenizeStep._tokenizer_identity", None), \
                 patch("src.steps.TokenizeStep._lemma_reading_cache", {"old": "reading"}) as readings, \
                 patch("src.steps.TokenizeStep.tokenizer_fingerprint", side_effect=lambda config: config.dictionary), \
                 patch("src.steps.TokenizeStep.dictionary.Dictionary") as factory:
                first = get_tokenizer(first_config)
                self.assertIs(first, get_tokenizer(first_config))
                self.assertEqual(readings, {})
                get_tokenizer(second_config)
                self.assertEqual(factory.call_count, 2)
                self.assertEqual(factory.call_args.kwargs, {"config_path": str(path), "dict": "small"})

    def test_tokenizer_supports_older_sudachi_dictionary_api(self):
        from types import SimpleNamespace

        analyzer = Mock()
        old_dictionary = SimpleNamespace(create=Mock(return_value=analyzer))
        with patch("src.steps.TokenizeStep._tokenizer", None), \
             patch("src.steps.TokenizeStep._tokenizer_identity", None), \
             patch("src.steps.TokenizeStep.dictionary.Dictionary", return_value=old_dictionary):
            self.assertIs(get_tokenizer(identity="older-sudachi"), analyzer)
        old_dictionary.create.assert_called_once_with()

    def test_changed_tokenizer_settings_do_not_reuse_prior_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source.txt"
            source.write_text("猫", encoding="utf-8")
            config_path = Path(tmp) / "sudachi.json"
            config_path.write_text("{}", encoding="utf-8")
            cache_dir = Path(tmp) / "cache"
            config_a = TokenizerConfig(config_path=str(config_path), dictionary="full")
            config_b = TokenizerConfig(config_path=str(config_path), dictionary="small")
            tokenizer = Mock()
            tokenizer.tokenize.return_value = [object()]
            with patch("src.steps.TokenizeStep.tokenizer_fingerprint", side_effect=lambda config: config.dictionary), \
                 patch("src.steps.TokenizeStep.appdirs.user_cache_dir", return_value=str(cache_dir)), \
                 patch("src.steps.TokenizeStep.get_tokenizer", return_value=tokenizer), \
                 patch("src.steps.TokenizeStep.sudachi_node_to_dict", return_value={"surface": "猫", "lemma": "猫", "base_form": "猫", "reading": "ねこ", "pos": ("名詞",)}):
                for config in (config_a, config_a, config_b):
                    Pipeline([RawTokenizeStep(config=config)]).run(Artifact([(source, ["猫"])], tmpdir=Path(tmp)))
            self.assertEqual(tokenizer.tokenize.call_count, 2)

    def test_forced_retokenization_refreshes_cached_tokens(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source.txt"
            source.write_text("猫", encoding="utf-8")
            cache_dir = Path(tmp) / "cache"
            cache = TokenCache(cache_dir, "version")
            old = {"surface": "old", "lemma": "old", "base_form": "old", "reading": "old", "pos": ("名詞",)}
            new = {"surface": "new", "lemma": "new", "base_form": "new", "reading": "new", "pos": ("名詞",)}
            cache.put(["猫"], [[old]])
            tokenizer = Mock()
            tokenizer.tokenize.return_value = [object()]
            cache_stats = {"hits": 0, "misses": 0, "disabled": 0}
            with patch("src.steps.TokenizeStep.get_tokenizer", return_value=tokenizer), \
                 patch("src.steps.TokenizeStep.sudachi_node_to_dict", return_value=new):
                cached = tokenize_raw_sentences(["猫"], tag="tag", source_path=source, cache=cache, cache_stats=cache_stats)
                self.assertEqual(cached[0].tokens, [old])
                tokenizer.tokenize.assert_not_called()
                refreshed = tokenize_raw_sentences(["猫"], tag="tag", source_path=source, cache=cache, force_retokenize=True, cache_stats=cache_stats)
                self.assertEqual(refreshed[0].tokens, [new])
                tokenizer.tokenize.assert_called_once()
                self.assertEqual(cache_stats, {"hits": 1, "misses": 1, "disabled": 0})
            reloaded = TokenCache(cache_dir, "version")
            with patch("src.steps.TokenizeStep.get_tokenizer") as get_tokenizer:
                result = tokenize_raw_sentences(["猫"], tag="tag", source_path=source, cache=reloaded)
            self.assertEqual(result[0].tokens, [new])
            get_tokenizer.assert_not_called()

    def test_completion_reports_hits_misses_refresh_and_disabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source.txt"
            source.write_text("猫", encoding="utf-8")
            token = {"surface": "猫", "lemma": "猫", "base_form": "猫", "reading": "ねこ", "pos": ("名詞",)}
            tokenizer = Mock()
            tokenizer.tokenize.return_value = [object()]
            events = []
            with patch("src.steps.TokenizeStep.tokenizer_fingerprint", return_value="version"), \
                 patch("src.steps.TokenizeStep.appdirs.user_cache_dir", return_value=tmp), \
                 patch("src.steps.TokenizeStep.get_tokenizer", return_value=tokenizer), \
                 patch("src.steps.TokenizeStep.sudachi_node_to_dict", return_value=token):
                for forced, tmpdir in ((False, Path(tmp)), (False, Path(tmp)), (True, Path(tmp)), (False, None)):
                    pipeline = Pipeline([RawTokenizeStep(force_retokenize=forced)],
                                        on_progress=lambda *a, **kw: events.append(kw["additional_text"]))
                    pipeline.run(Artifact([(source, ["猫"])], tmpdir=tmpdir))
            self.assertIn("0 hits, 1 misses", events[0])
            self.assertIn("1 hits, 0 misses", events[1])
            self.assertIn("1 refreshed files", events[2])
            self.assertIn("cache disabled", events[3])

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