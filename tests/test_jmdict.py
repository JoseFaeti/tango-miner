import tempfile
import unittest
from pathlib import Path

from src.JMDict import JMDict


XML = """<?xml version="1.0" encoding="UTF-8"?>
<JMdict>
  <entry>
    <k_ele>
      <keb>猫</keb>
      <ke_pri>nf01</ke_pri>
    </k_ele>
    <r_ele>
      <reb>ねこ</reb>
      <re_pri>ichi1</re_pri>
    </r_ele>
    <sense>
      <pos>noun (common) (futsuumeishi)</pos>
      <gloss>cat</gloss>
    </sense>
    <sense>
      <pos>noun (common) (futsuumeishi)</pos>
      <gloss>feline</gloss>
    </sense>
  </entry>
  <entry>
    <k_ele>
      <keb>猫</keb>
      <ke_pri>nf48</ke_pri>
    </k_ele>
    <r_ele>
      <reb>ねこ</reb>
    </r_ele>
    <sense>
      <gloss>less common cat entry</gloss>
    </sense>
  </entry>
</JMdict>
"""


class JMDictTests(unittest.TestCase):
    def build_dict(self):
        tmp = tempfile.TemporaryDirectory()
        path = Path(tmp.name) / "JMdict_e.xml"
        path.write_text(XML, encoding="utf-8")
        return tmp, JMDict(path)

    def test_lookup_indexes_kanji_and_reading_forms(self):
        tmp, jmdict = self.build_dict()
        self.addCleanup(tmp.cleanup)

        self.assertEqual(len(jmdict.lookup_word("猫")), 2)
        self.assertEqual(len(jmdict.lookup_word("ねこ")), 2)
        self.assertEqual(jmdict.lookup_word("犬"), [])

    def test_get_best_entry_prefers_priority_tags(self):
        tmp, jmdict = self.build_dict()
        self.addCleanup(tmp.cleanup)

        best = jmdict.get_best_entry("猫")

        self.assertEqual(best["senses"][0]["gloss"], ["cat"])

    def test_get_most_common_definition_formats_all_senses(self):
        tmp, jmdict = self.build_dict()
        self.addCleanup(tmp.cleanup)

        definition = jmdict.get_most_common_definition("猫")

        self.assertEqual(definition, "1. cat<br>2. feline")
        self.assertIsNone(jmdict.get_most_common_definition("犬"))


if __name__ == "__main__":
    unittest.main()
