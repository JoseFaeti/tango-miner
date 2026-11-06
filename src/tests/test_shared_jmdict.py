import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.JMDict import JMDict
from src.SharedJMDict import SharedIndex, _valid, prepare_shared_index


class SharedJMDictTests(unittest.TestCase):
    def test_read_only_index_exact_lookup_collision_and_corruption_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'dictionary.xml'
            source.write_text('''<JMdict>
              <entry><ent_seq>1</ent_seq><k_ele><keb>語彙</keb></k_ele>
                <r_ele><reb>ごい</reb></r_ele><sense><pos>noun (common)</pos><gloss>vocabulary</gloss></sense></entry>
              <entry><ent_seq>2</ent_seq><r_ele><reb>から</reb></r_ele>
                <sense><pos>noun (common)</pos><gloss>shell</gloss></sense></entry>
              </JMdict>''', encoding='utf-8')
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            reference = JMDict(source)
            with patch('src.SharedJMDict.zlib.crc32', return_value=123):
                path = prepare_shared_index(source, digest, root)
                self.assertTrue(_valid(path, bytes.fromhex(digest)))
                index = SharedIndex(path, digest)
                try:
                    for word in (*reference.index, '存在しない'):
                        with self.subTest(word=word):
                            self.assertEqual(index.get(word, []), reference.index.get(word, []))
                    self.assertEqual(index.get(chr(0xd800), []), [])
                    with self.assertRaises(TypeError):
                        index.mapping[0] = 0
                finally:
                    index.close()
                with patch('src.steps.MergeTokensStep.index.get_jmdict', side_effect=AssertionError('rebuilt')):
                    self.assertEqual(prepare_shared_index(source, digest, root), path)
                with path.open('r+b') as stream:
                    stream.seek(-1, 2)
                    byte = stream.read(1)
                    stream.seek(-1, 2)
                    stream.write(bytes([byte[0] ^ 1]))
                self.assertFalse(_valid(path, bytes.fromhex(digest)))
                self.assertEqual(prepare_shared_index(source, digest, root), path)
                self.assertTrue(_valid(path, bytes.fromhex(digest)))
                restored = SharedIndex(path, digest)
                try:
                    self.assertEqual(restored.get('語彙', []), reference.index['語彙'])
                finally:
                    restored.close()


if __name__ == '__main__':
    unittest.main()
