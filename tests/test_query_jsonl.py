import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from scripts.query_jsonl import main


class QueryJsonlTests(unittest.TestCase):
    def test_nested_array_search_and_bounded_results(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'diagnostics.jsonl'
            rows = [{'sentence': '見る', 'proposals': [{'surface': 'ている'}]},
                    {'sentence': '読む', 'proposals': [{'surface': 'ている'}]},
                    {'sentence': '話す', 'proposals': []}]
            path.write_text(''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in rows), encoding='utf-8')
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = main([str(path), '--field', 'proposals.surface', '--exact', 'ている',
                               '--show', 'sentence', '--limit', '1'])
            self.assertEqual(result, 0)
            self.assertIn('見る', output.getvalue())
            self.assertNotIn('読む', output.getvalue())
            self.assertIn('Matches: 2; shown: 1', output.getvalue())

    def test_invalid_json_reports_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'bad.jsonl'
            path.write_text('{}\ninvalid\n', encoding='utf-8')
            error = io.StringIO()
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(error):
                self.assertEqual(main([str(path), '--contains', 'x']), 1)
            self.assertIn(':2:', error.getvalue())


if __name__ == '__main__':
    unittest.main()
