# 〜てほしい

Status: [~]

## Meaning

Want someone to do.

## Implemented scope

The rule matches an adjacent verb or auxiliary in continuative form followed
by the connective て or で and the adjective ほしい / 欲しい. It includes
the base, past, negative, conditional and continuative forms supported by the
shared adjective helper, and an adjacent です. The candidate span begins at
the connective; the verb remains available as lexical evidence.

Focused regressions cover synthetic token boundaries and source-aligned Sudachi
cases for base, past, negative, conditional and polite forms, lexical controls,
reservation and card integration. Direct inspection of Sudachi full dictionary
split mode C confirmed the tokenization of those examples, including separate
connective and adjective tokens. The worker's full test suite and grammar documentation structural check both
passed after the second repair (exit code 0). These checks do not establish
coverage of every variant or semantic context. Less common variants remain unvalidated and forms outside the
specified token patterns may be missed. Semantic context is not resolved, so
an apparent desire for an event may also be matched.
