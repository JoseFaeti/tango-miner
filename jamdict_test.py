#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
from tango_miner import get_most_common_definition, get_jamdict, best_entries, enable_debug_logging


class DefinitionTest(unittest.TestCase):
  def test_common_kana_definitions(self):
    self.assertTrue('nicely; properly' in get_most_common_definition('よく'))
    self.assertTrue('in this way' in get_most_common_definition('こう'))
    self.assertTrue('to go' in get_most_common_definition('いく'))
    self.assertTrue('in what way' in get_most_common_definition('どう'))
    self.assertTrue('again; and; also; still' in get_most_common_definition('また'))
    self.assertTrue('corner; nook; recess' in get_most_common_definition('すみ'))
    self.assertTrue('jest; joke; funny story' in get_most_common_definition('じょうだん'))

  def test_uncommon_definitions(self):
    result = get_jamdict().lookup('すれ')
    self.assertEqual(len(best_entries(result.entries, 'すれ', tie_break="defs")), 0)

    # Problematic ones to test next:
    # かけ
    # こと



if __name__ == '__main__':
  enable_debug_logging()
  unittest.main()