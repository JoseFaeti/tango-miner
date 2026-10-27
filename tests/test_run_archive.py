import hashlib
import json
import subprocess
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from src.Artifact import Artifact
from src.RunArchive import create_run_archive, collect_dependencies, write_run_summary
from src.RunConfig import RunConfig, TokenizerConfig, DictionaryConfig
from dataclasses import replace
from unittest.mock import patch
from src.steps.GatherInputFilesStep import GatherInputFilesStep


class RunArchiveTests(unittest.TestCase):
    def test_git_snapshot_and_same_minute_runs_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            def git(*args):
                return subprocess.run(["git", "-C", str(repo), *args], check=True,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            git("init")
            git("config", "user.email", "test@example.invalid")
            git("config", "user.name", "Test")
            (repo / "tracked.txt").write_text("before\n", encoding="utf-8")
            git("add", "tracked.txt")
            git("commit", "-m", "baseline")
            commit = git("rev-parse", "HEAD").stdout.decode().strip()
            (repo / "tracked.txt").write_text("after\n", encoding="utf-8")
            (repo / "new.txt").write_text("new file\n", encoding="utf-8")
            output = Path(tmp) / "output" / "anki.csv"
            now = datetime(2026, 10, 27, 11, 30)
            first = create_run_archive(output, RunConfig(), repo=repo, now=now)
            second = create_run_archive(output, RunConfig(), repo=repo, now=now)
            self.assertEqual(first.parent.name, f"2026-10-27_1130_{commit[:7]}-dirty")
            self.assertEqual(second.parent.name, first.parent.name + "_2")
            patch = (first.parent / "changes.patch").read_text(encoding="utf-8")
            self.assertIn("+after", patch)
            self.assertIn("+new file", patch)
            self.assertEqual(json.loads((first.parent / "config.json").read_text()), json.loads(RunConfig().to_json()))
            self.assertIn(commit, (first.parent / "notes.txt").read_text())
            git("reset", "--hard", "HEAD")
            (repo / "new.txt").unlink()
            subprocess.run(["git", "-C", str(repo), "apply", str(first.parent / "changes.patch")], check=True)
            self.assertEqual((repo / "tracked.txt").read_text(), "after\n")
            self.assertEqual((repo / "new.txt").read_text(), "new file\n")

    def test_dependencies_hash_custom_dictionary_and_configuration_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            system = root / "system.dic"
            user = root / "user.dic"
            jm = root / "jmdict.xml"
            sudachi = root / "sudachi.json"
            for path in (system, user, jm):
                path.write_bytes(path.name.encode())
            sudachi.write_text(json.dumps({"userDict": [str(user)]}), encoding="utf-8")
            config = replace(RunConfig(), tokenizer=TokenizerConfig(str(sudachi), str(system)),
                             dictionary=DictionaryConfig(str(jm)))
            result = collect_dependencies(config)
            self.assertEqual(result["files"]["sudachi_dictionary"][0]["sha256"], hashlib.sha256(b"system.dic").hexdigest())
            self.assertEqual(result["files"]["jmdict"]["sha256"], hashlib.sha256(b"jmdict.xml").hexdigest())
            self.assertEqual(result["files"]["configured_sudachi_dictionaries"][0]["sha256"], hashlib.sha256(b"user.dic").hexdigest())
            self.assertTrue(result["python"])
            system.unlink()
            self.assertIn("error", collect_dependencies(config)["files"]["sudachi_dictionary"][0])

    def test_summary_replaces_prior_running_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_run_summary(tmp, {"status": "running"})
            write_run_summary(tmp, {"status": "failed", "error": "example"})
            self.assertEqual(json.loads((Path(tmp) / "summary.json").read_text()),
                             {"status": "failed", "error": "example"})

    def test_missing_git_is_reported_and_does_not_prevent_archiving(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = create_run_archive(Path(tmp) / "anki.csv", RunConfig(), repo=Path(tmp),
                                   now=datetime(2026, 10, 27, 11, 30))
            self.assertEqual(p.parent.name, "2026-10-27_1130_unknown")
            self.assertIn("Git snapshot unavailable", (p.parent / "notes.txt").read_text())

    def test_old_archives_are_excluded_when_output_location_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            corpus = root / "input"
            corpus.mkdir()
            source = corpus / "source.txt"
            source.write_text("source", encoding="utf-8")
            archive = corpus / "runs" / "2026-10-27_1143_unknown"
            archive.mkdir(parents=True)
            for name in ("config.json", "notes.txt", "input.csv", "input.dropped.csv", "input.sentence.csv"):
                (archive / name).write_text("previous output", encoding="utf-8")
            # A normal corpus folder named runs is not itself an archive.
            genuine = corpus / "runs" / "stories.txt"
            genuine.write_text("a story", encoding="utf-8")
            nested = corpus / "another" / "runs" / "2026-10-27_1144_1234567-dirty_2"
            nested.mkdir(parents=True)
            for name in ("config.json", "notes.txt", "input.csv"):
                (nested / name).write_text("previous output", encoding="utf-8")
            unrelated = corpus / "runs" / "2026-10-27_1145_ordinary"
            unrelated.mkdir()
            (unrelated / "story.txt").write_text("real input", encoding="utf-8")
            output_root = root / "elsewhere" / "runs"
            manifest = root / "manifest.json"
            step = GatherInputFilesStep(corpus, include_subdirectories=True,
                                        excluded_directory=output_root, manifest_path=manifest)
            files = step.process(Artifact(None)).data
            self.assertEqual(set(files), {source, genuine, unrelated / "story.txt"})
            entries = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual({entry["path"] for entry in entries}, {str(f.resolve()) for f in files})
            # An explicitly selected file remains an input even inside an archive.
            selected = archive / "input.csv"
            self.assertEqual(GatherInputFilesStep(selected).process(Artifact(None)).data, [selected])

    def test_recursive_input_excludes_archives_and_hashes_selected_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.txt"
            source.write_text("source", encoding="utf-8")
            old = root / "runs" / "old"
            old.mkdir(parents=True)
            (old / "anki.csv").write_text("old output", encoding="utf-8")
            manifest = old / "input-manifest.json"
            step = GatherInputFilesStep(root, include_subdirectories=True,
                                        excluded_directory=root / "runs", manifest_path=manifest)
            result = step.process(Artifact(None))
            self.assertEqual(result.data, [source])
            entries = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual(entries, [{"path": str(source.resolve()),
                                        "sha256": hashlib.sha256(b"source").hexdigest()}])


if __name__ == "__main__":
    unittest.main()
