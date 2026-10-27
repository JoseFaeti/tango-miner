import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from src.RunConfig import RunConfig, resolve_config


class RunConfigTests(unittest.TestCase):
    def test_default_config_is_loaded_by_cli(self):
        config = resolve_config('config.toml')
        result = subprocess.run([sys.executable, 'tango_miner.py', '--show-config'], capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(config.to_json()), json.loads(result.stdout))

    def test_file_and_explicit_overrides(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.toml'
            path.write_text('config_version = 1\n[input]\nrecursive = true\nencoding = "utf-16"\n[filter]\nmin_frequency = 8\n', encoding='utf-8')
            config = resolve_config(path, {'filter': {'min_frequency': 2}})
        self.assertTrue(config.input.recursive)
        self.assertEqual('utf-16', config.input.encoding)
        self.assertEqual(2, config.filter.min_frequency)
        self.assertEqual(RunConfig().output, config.output)

    def test_invalid_config_rejected(self):
        for contents in ('config_version = 2', '[filter]\nmin_frequency = 0', '[input]\nrecursiv = true', '[filter]\nmin_frequency = true', '[anki]\nenabled = 1', '[anki]\noutput_only = 1', '[filter]\nkeep_percent = 0', '[filter]\nmin_frequency_per_source = -1', '[input]\nextensions = [1]', '[scoring]\ntag_saturation = 1', '[scoring]\nfrequency_weight = 0\nposition_weight = 0', '[scoring]\nmax_score = inf', '[scoring]\nminimum_tag_factor = 1.1', '[sentences]\nmax_sentences = 0', '[sentences]\nlength_divisor = 0', '[sentences]\nunknown_word_penalty = -1', '[sentences]\nvariance_divisor = inf', '[sentences]\nmin_word_count = true', '[merge]\nmax_lookahead = 1', '[merge]\nmax_lookahead = true', '[dictionary]\njmdict_path = ""', '[anki]\nendpoint = "nope"', '[anki]\ntimeout_seconds = 0', '[anki]\nbatch_size = 0', '[anki]\nnote_fetch_batch_size = true', '[anki]\ndeck = " "', '[tokenizer]\nconfig_path = ""', '[tokenizer]\ndictionary = true'):
            with self.subTest(contents=contents), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'config.toml'
                path.write_text(contents, encoding='utf-8')
                with self.assertRaises(ValueError):
                    resolve_config(path)

    def test_tokenizer_settings_load_from_toml(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.toml'
            path.write_text('[tokenizer]\nconfig_path = "custom.json"\ndictionary = "small"\n', encoding='utf-8')
            tokenizer = resolve_config(path).tokenizer
        self.assertEqual((tokenizer.config_path, tokenizer.dictionary), ("custom.json", "small"))

    def test_dictionary_path_loads_from_toml(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.toml'
            path.write_text('[dictionary]\njmdict_path = "other.xml"\n', encoding='utf-8')
            self.assertEqual(resolve_config(path).dictionary.jmdict_path, 'other.xml')
        self.assertEqual(resolve_config().dictionary.jmdict_path, '~/JMdict_e.xml')

    def test_merge_lookahead_loads_from_toml(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.toml'
            path.write_text('[merge]\nmax_lookahead = 4\n', encoding='utf-8')
            self.assertEqual(resolve_config(path).merge.max_lookahead, 4)
        self.assertEqual(resolve_config().merge.max_lookahead, 10)

    def test_sentence_settings_load_from_toml(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.toml'
            path.write_text('[sentences]\nmax_sentences = 2\nideal_length = 18\nunknown_word_penalty = 0.3\n', encoding='utf-8')
            config = resolve_config(path)
        self.assertEqual(config.sentences.max_sentences, 2)
        self.assertEqual(config.sentences.ideal_length, 18)
        self.assertEqual(config.sentences.unknown_word_penalty, 0.3)
        self.assertEqual(config.sentences.variance_divisor, 100000)

    def test_scoring_settings_load_from_toml(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.toml'
            path.write_text('[scoring]\nfrequency_weight = 0.2\nposition_weight = 0.8\ntag_saturation = 5\n', encoding='utf-8')
            config = resolve_config(path)
        self.assertEqual(config.scoring.frequency_weight, 0.2)
        self.assertEqual(config.scoring.position_weight, 0.8)
        self.assertEqual(config.scoring.tag_saturation, 5)
        self.assertEqual(config.scoring.max_score, 1000)

    def test_runtime_policies_are_loaded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.toml'
            path.write_text('[input]\nextensions = [".TXT"]\n[filter]\nkeep_percent = 75\nmin_frequency_per_source = 2.5\n', encoding='utf-8')
            config = resolve_config(path)
        self.assertEqual(config.input.extensions, ('.txt',))
        self.assertEqual(config.filter.keep_percent, 75)
        self.assertEqual(config.filter.min_frequency_per_source, 2.5)

    def test_anki_connection_settings_load_from_toml(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.toml'
            path.write_text('[anki]\nendpoint = "http://localhost:1234"\ndeck = "Test"\nmodel = "Custom"\ntimeout_seconds = 2\nbatch_size = 3\nnote_fetch_batch_size = 5\n', encoding='utf-8')
            anki = resolve_config(path).anki
        self.assertEqual((anki.endpoint, anki.deck, anki.model), ("http://localhost:1234", "Test", "Custom"))
        self.assertEqual((anki.timeout_seconds, anki.batch_size, anki.note_fetch_batch_size), (2, 3, 5))

    def test_anki_is_disabled_by_default_and_can_be_enabled(self):
        self.assertFalse(resolve_config().anki.enabled)
        self.assertTrue(resolve_config().anki.output_only)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.toml'
            path.write_text('[anki]\nenabled = true\n', encoding='utf-8')
            self.assertTrue(resolve_config(path).anki.enabled)

    def test_show_config_cli_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.toml'
            path.write_text('[filter]\nmin_frequency = 8\n[input]\nrecursive = true\n', encoding='utf-8')
            result = subprocess.run([sys.executable, 'tango_miner.py', '--config', str(path), '--minFrequency', '3', '--show-config'], capture_output=True, text=True, check=True)
        resolved = json.loads(result.stdout)
        self.assertEqual(3, resolved['filter']['min_frequency'])
        self.assertTrue(resolved['input']['recursive'])


if __name__ == '__main__':
    unittest.main()
