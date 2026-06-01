import unittest

from src.TokenizeStep import is_japanese_char, is_useless, kata_to_hira


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


if __name__ == "__main__":
    unittest.main()
