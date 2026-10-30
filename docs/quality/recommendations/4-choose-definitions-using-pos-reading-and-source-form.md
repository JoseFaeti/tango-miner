# 4. Choose definitions using POS, reading, and source form

[Contents](../recommended-improvements-in-priority-order.md)


Dictionary selection currently favours JMdict frequency metadata without enough
evidence from the token's POS, source spelling, inflection, and context. This
produces definitions that visibly disagree with their examples, including
`ようやく`, `好く`, and `きげん`.

Prefer an exact spelling match and compatible POS before applying frequency
priorities. Retain the token reading and surface form through lookup so that
homophones can be distinguished.

