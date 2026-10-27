import tempfile
import unittest
from pathlib import Path

from src.JMDict import JMDict
from src.TokenProvenance import TokenComponent
from unittest.mock import Mock


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
    def test_definitions_respect_known_spelling_and_reading_restrictions(self):
        xml = """<JMdict><entry>
        <k_ele><keb>表記</keb></k_ele><k_ele><keb>別記</keb></k_ele>
        <r_ele><reb>ひょうき</reb></r_ele><r_ele><reb>べっき</reb></r_ele>
        <sense><gloss>shared</gloss></sense>
        <sense><stagk>別記</stagk><gloss>other spelling</gloss></sense>
        <sense><stagr>べっき</stagr><gloss>other reading</gloss></sense>
        </entry><entry><k_ele><keb>限定</keb></k_ele><k_ele><keb>他</keb></k_ele>
        <sense><stagk>他</stagk><gloss>only other</gloss></sense></entry></JMdict>"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dict.xml"
            path.write_text(xml, encoding="utf-8")
            d = JMDict(path)
            self.assertEqual(d.get_most_common_definition("表記"), "1. shared<br>2. other reading")
            self.assertEqual(d.get_most_common_definition("ひょうき"), "1. shared<br>2. other spelling")
            self.assertEqual(d.get_most_common_definition("別記"), "1. shared<br>2. other spelling<br>3. other reading")
            self.assertIsNone(d.get_most_common_definition("限定"))

    def build_dict(self):
        tmp = tempfile.TemporaryDirectory()
        path = Path(tmp.name) / "JMdict_e.xml"
        path.write_text(XML, encoding="utf-8")
        return tmp, JMDict(path)

    def test_short_kana_context_uses_unanimous_pos_without_guessing_homophones(self):
        xml = """<JMdict>
        <entry><r_ele><reb>まつ</reb><re_pri>nf01</re_pri></r_ele><sense><pos>noun (common)</pos><gloss>pine</gloss></sense></entry>
        <entry><r_ele><reb>まつ</reb></r_ele><sense><pos>Godan verb</pos><gloss>wait</gloss></sense></entry>
        <entry><r_ele><reb>そば</reb></r_ele><sense><pos>noun (common)</pos><gloss>noodles</gloss></sense></entry>
        <entry><r_ele><reb>そば</reb></r_ele><sense><pos>noun (common)</pos><gloss>nearby</gloss></sense></entry>
        <entry><r_ele><reb>たい</reb><re_pri>nf01</re_pri></r_ele><sense><pos>noun (common)</pos><gloss>fish</gloss></sense></entry>
        <entry><r_ele><reb>たい</reb></r_ele><sense><pos>auxiliary adjective</pos><gloss>want to</gloss></sense><sense><gloss>wish to</gloss></sense><sense><pos>particle</pos><gloss>emphasis</gloss></sense></entry>
        </JMdict>"""
        def component(word, pos):
            return (TokenComponent(word, word, word, word, (pos, "*")),)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dict.xml"
            path.write_text(xml, encoding="utf-8")
            d = JMDict(path)
            self.assertIsNone(d.get_most_common_definition("まつ", component_counts={component("まつ", "動詞"): 2}))
            self.assertEqual(d.get_most_common_definition("たい", component_counts={component("たい", "助動詞"): 2}), "1. want to<br>2. wish to")
            self.assertIsNone(d.get_most_common_definition("そば", component_counts={component("そば", "名詞"): 2}))
            self.assertIsNone(d.get_most_common_definition("まつ", component_counts={component("まつ", "名詞"): 2}))
            self.assertIsNone(d.get_most_common_definition("まつ", component_counts={component("まつ", "動詞"): 2, component("まつ", "名詞"): 1}))
            self.assertIsNone(d.get_most_common_definition("まつ"))

    def test_potential_fallback_checks_reading_morphology_and_evidence(self):
        xml = """<JMdict>
        <entry><k_ele><keb>倒す</keb></k_ele><r_ele><reb>たおす</reb></r_ele><sense><pos>transitive verb</pos><gloss>defeat</gloss></sense></entry>
        <entry><k_ele><keb>倒す</keb></k_ele><r_ele><reb>こかす</reb></r_ele><sense><pos>transitive verb</pos><gloss>knock over</gloss></sense></entry>
        </JMdict>"""
        node = Mock()
        node.dictionary_form.return_value = "倒せる"
        node.normalized_form.return_value = "倒す"
        tokenizer = Mock()
        tokenizer.tokenize.return_value = [node]
        def evidence(reading="たおせる", pos="動詞", lemma="倒せる"):
            return {(TokenComponent("倒せる", lemma, lemma, reading, (pos, "*")),): 1}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dict.xml"
            path.write_text(xml, encoding="utf-8")
            d = JMDict(path)
            self.assertEqual(d.get_potential_definition("倒せる", evidence(), tokenizer), "Potential form of 倒す: defeat")
            stem_only = {(TokenComponent("倒せ", "倒せる", "倒せる", "たおせる", ("動詞", "*")),): 1}
            self.assertIsNone(d.get_potential_definition("倒せる", stem_only, tokenizer))
            self.assertIsNone(d.get_potential_definition("倒せる", evidence("ふせる"), tokenizer))
            self.assertIsNone(d.get_potential_definition("倒せる", evidence(pos="名詞"), tokenizer))
            self.assertIsNone(d.get_potential_definition("倒せる", evidence(lemma="別"), tokenizer))
            node.normalized_form.return_value = "倒れる"
            self.assertIsNone(d.get_potential_definition("倒せる", evidence(), tokenizer))
            tokenizer.tokenize.return_value = [node, node]
            self.assertIsNone(d.get_potential_definition("倒せる", evidence(), tokenizer))

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
