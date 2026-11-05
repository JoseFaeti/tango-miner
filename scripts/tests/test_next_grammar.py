import tempfile
import unittest
from pathlib import Path

from scripts.next_grammar import next_point, select_next


class NextGrammarTests(unittest.TestCase):
    def test_first_unchecked_point_skips_other_states(self):
        queue = ('- [~] [partial](points/partial.md)\n'
                 '- [x] [done](points/done.md)\n'
                 '- [ ] [〜ために](points/tame-ni.md)\n'
                 '- [ ] [later](points/later.md)\n')
        self.assertEqual(next_point(queue), {
            'name': '〜ために', 'path': 'docs/grammar/points/tame-ni.md',
        })

    def test_queue_priority_and_exhaustion(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            queues = {
                'next.md': '- [ ] [next](points/next.md)\n',
                'later.md': '- [ ] [later](points/later.md)\n',
                'partially-implemented.md': '- [~] [partial](points/partial.md)\n',
            }
            for filename, contents in queues.items():
                (directory / filename).write_text(contents, encoding='utf-8')
            for filename, name in (("next.md", "next"), ("later.md", "later"),
                                   ("partially-implemented.md", "partial")):
                self.assertEqual(select_next(directory), {
                    'name': name, 'path': f'docs/grammar/points/{name}.md',
                })
                (directory / filename).write_text('- [x] [done](points/done.md)\n', encoding='utf-8')
            self.assertIsNone(select_next(directory))

    def test_no_unchecked_point(self):
        self.assertIsNone(next_point('- [~] [partial](points/partial.md)\n'))
