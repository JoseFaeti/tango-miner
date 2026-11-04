# 〜はずだった

Status: [~]

## Meaning

Contrary expectation.

## Supported forms and remaining work

The [〜はずだ](hazu-da.md) rule supports selected past copulas, including
the tested `寒いはずだった`. It does not classify a contrary-expectation
meaning, and other attachments and tokenizations remain unsupported. The
earlier backlog marked this family `[ ]`; the tested past-form coverage
warrants `[~]` without implying that this semantic scope is complete.

## Implementation

- Rule: [hazu_da.py](../../../src/grammar_rules/rules/hazu_da.py)
- Tests: [test_grammar_hazu_da.py](../../../src/grammar_rules/rules/tests/test_grammar_hazu_da.py)
- Registry identity: `hazu_da`
