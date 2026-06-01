import unittest

from src.TokenizeStep import (
    SENT_BOUNDARY,
    clean_sentence_text,
    contains_japanese_script,
    is_japanese_char,
    is_useless,
    iter_sudachi_chunks,
    kata_to_hira,
    split_text_by_utf8_bytes,
)


def token(lemma, pos=("名詞", "普通名詞", "一般", "*"), reading="ヨミ"):
    return {
        "base_form": lemma,
        "lemma": lemma,
        "reading": reading,
        "pos": pos,
    }


class TokenizeHelperTests(unittest.TestCase):
    def test_kata_to_hira_converts_katakana_reading(self):
        self.assertEqual(kata_to_hira("カタカナー"), "かたかなー")
        self.assertIsNone(kata_to_hira(""))

    def test_is_useless_filters_function_words_and_proper_nouns(self):
        self.assertTrue(is_useless(token("は", pos=("助詞", "係助詞", "*", "*"))))
        self.assertTrue(is_useless(token("東京", pos=("名詞", "固有名詞", "地名", "*"))))

    def test_is_useless_filters_noise_but_keeps_regular_japanese_words(self):
        self.assertTrue(is_useless(token("あ")))
        self.assertTrue(is_useless(token("スーパー")))
        self.assertTrue(is_useless(token("言っ")))
        self.assertFalse(is_useless(token("言葉")))

    def test_is_japanese_char_accepts_expected_sentence_characters(self):
        for ch in ["猫", "ね", "ネ", "A", "５", "。", "「"]:
            self.assertTrue(is_japanese_char(ch), ch)

        self.assertFalse(is_japanese_char("😀"))

    def test_split_text_by_utf8_bytes_keeps_chunks_under_limit(self):
        chunks = list(split_text_by_utf8_bytes("言葉abc", max_bytes=7))

        self.assertEqual("".join(chunks), "言葉abc")
        self.assertTrue(all(len(chunk.encode("utf-8")) <= 7 for chunk in chunks))

    def test_iter_sudachi_chunks_splits_oversized_sentence_parts(self):
        text = "言葉" * 10
        chunks = list(iter_sudachi_chunks(text, max_bytes=12))

        self.assertEqual("".join(chunks), text + SENT_BOUNDARY)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk.encode("utf-8")) <= 12 for chunk in chunks))

    def test_clean_sentence_text_strips_non_japanese_script_prefix(self):
        self.assertEqual(
            clean_sentence_text("F4　AD　01　01　10　01　01　01>美鶴は切なげに微笑んだ。"),
            "美鶴は切なげに微笑んだ。",
        )

    def test_clean_sentence_text_preserves_japanese_prefix_before_gt(self):
        self.assertEqual(
            clean_sentence_text("美鶴>切なげに微笑んだ。"),
            "美鶴>切なげに微笑んだ。",
        )

    def test_contains_japanese_script_ignores_ascii_metadata(self):
        self.assertFalse(contains_japanese_script("F4 AD 01"))
        self.assertTrue(contains_japanese_script("美鶴"))


if __name__ == "__main__":
    unittest.main()
