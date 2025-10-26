#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
from tango_miner import get_most_common_definition, get_jamdict, best_entries


class DefinitionTest(unittest.TestCase):
  def test_common_kana_definitions(self):
    self.assertTrue('in this way' in get_most_common_definition('こう'))
    self.assertTrue('to go' in get_most_common_definition('いく'))
    self.assertTrue('in what way' in get_most_common_definition('どう'))
    self.assertTrue('nicely/properly/well/skillfully' in get_most_common_definition('よく'))
    self.assertTrue('again/and/also/still' in get_most_common_definition('また'))
    self.assertTrue('corner/nook/recess' in get_most_common_definition('すみ'))


  def test_uncommon_definitions(self):
    result = get_jamdict().lookup('すれ')
    self.assertEquals(len(best_entries(result.entries, 'すれ', tie_break="defs")), 0)

    # Problematic ones to test next:
    # かけ



if __name__ == '__main__':
  unittest.main()