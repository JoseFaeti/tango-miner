import unittest

from src.steps.TokenizeStep import (
    contains_japanese_script,
    is_japanese_char,
    is_useless,
    kata_to_hira,
)


def token(lemma, pos=("名詞", "普通名詞", "一般", "*"), reading="ヨミ"):
    return {
        "base_form": lemma,
        "lemma": lemma,
        "reading": reading,
        "pos": pos,
    }


class TokenizeHelperTests(unittest.TestCase):
    def test_kata_to_hira_converts_katakana_readings(self):
        self.assertEqual(kata_to_hira("カタカナー"), "かたかなー")
        self.assertEqual(kata_to_hira(""), "")

    def test_is_useless_filters_function_words_and_noise(self):
        self.assertTrue(is_useless(token("は", pos=("助詞", "係助詞", "*", "*"))))
        self.assertTrue(is_useless(token("スーパー")))
        self.assertTrue(is_useless(token("言っ")))
        self.assertFalse(is_useless(token("しよう")))
        self.assertFalse(is_useless(token("てく", pos=("助動詞", "*", "*", "*"))))
        self.assertFalse(is_useless(token("いる", pos=("動詞", "非自立可能", "*", "*"))))
        self.assertFalse(is_useless(token("ようやく", pos=("副詞", "*", "*", "*"))))
        self.assertFalse(is_useless(token("おもいで")))
        self.assertFalse(is_useless(token("言葉")))

    def test_japanese_script_helpers_distinguish_supported_characters(self):
        self.assertTrue(contains_japanese_script("美鶴"))
        self.assertFalse(contains_japanese_script("F4 AD 01"))
        self.assertTrue(is_japanese_char("猫"))
        self.assertTrue(is_japanese_char("ね"))
        self.assertTrue(is_japanese_char("A"))


if __name__ == "__main__":
    unittest.main()
