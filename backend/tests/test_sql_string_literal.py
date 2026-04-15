"""Tests for ``_sql_string_literal``.

It's the last line of defence when we have to interpolate a pre-validated
value into a DuckDB statement that doesn't accept ``?`` placeholders
(CREATE SECRET, ATTACH). It must double any embedded single quote and
reject control characters.
"""

import pytest

from main import _sql_string_literal


def test_plain_value_quoted():
    assert _sql_string_literal("plain") == "'plain'"


def test_single_quote_doubled():
    assert _sql_string_literal("O'Brien") == "'O''Brien'"
    assert _sql_string_literal("a';--") == "'a'';--'"


def test_tab_allowed():
    assert _sql_string_literal("a\tb") == "'a\tb'"


@pytest.mark.parametrize("bad", ["has\x00null", "has\nnewline", "has\rreturn", "\x01"])
def test_control_chars_rejected(bad):
    with pytest.raises(ValueError):
        _sql_string_literal(bad)
