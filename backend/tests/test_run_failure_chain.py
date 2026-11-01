"""A failure line can say why, and cannot say anything private."""

import pytest

from deerflow.runtime.runs.terminal import failure_chain

HERE = "test_run_failure_chain.py"


def raised(error):
    try:
        raise error
    except BaseException as caught:
        return caught


def test_a_cause_hidden_by_from_none_is_still_named():
    def source():
        raise KeyError("private")

    def proof():
        try:
            source()
        except KeyError:
            raise ValueError("publication-source-conflict") from None

    with pytest.raises(ValueError) as caught:
        proof()

    outer, inner = failure_chain(caught.value).split(">")
    assert outer.startswith(f"ValueError:publication-source-conflict:{HERE}:")
    assert inner.startswith(f"KeyError:{HERE}:")


def test_an_explicit_cause_wins_over_the_context():
    error = raised(ValueError("not a fixed word"))
    try:
        try:
            raise OSError("context")
        except OSError:
            raise RuntimeError("wrapped-failure") from error
    except RuntimeError as caught:
        assert ">ValueError:" in failure_chain(caught) and "OSError" not in failure_chain(caught)


@pytest.mark.parametrize(
    "message",
    [
        "123456789:AAEhBOweik6ad9r-synthetic-token",
        "person@example.com",
        "SELECT secret FROM users",
        "Connection refused by db.internal",
        "https://example.com/private",
        "/mnt/user-data/outputs/report.txt",
        "publication-conflict: person@example.com",
        "report.txt",
        "hooks.example.com",
        "long-" + "x" * 80,
        "maria",
        "",
    ],
)
@pytest.mark.parametrize("kind", [ValueError, RuntimeError])
def test_exception_text_never_prints_unless_it_is_a_fixed_word(kind, message):
    line = failure_chain(raised(kind(message)))
    assert line.startswith(f"{kind.__name__}:{HERE}:")
    assert message == "" or message not in line


@pytest.mark.parametrize(
    "error",
    [
        KeyError("looks-like-a-word"),
        OSError("looks-like-a-word"),
        ValueError("looks-like-a-word", "another-argument"),
        ValueError(("looks-like-a-word",)),
        type("Subclass", (ValueError,), {})("looks-like-a-word"),
    ],
)
def test_only_a_single_word_of_the_exact_builtin_types_prints(error):
    assert "looks-like-a-word" not in failure_chain(raised(error))


def test_a_hostile_exception_is_never_asked_to_describe_itself():
    class Hostile(Exception):
        def __str__(self):
            raise AssertionError("must not run")

        __repr__ = __str__

    assert failure_chain(raised(Hostile("private"))).startswith(f"Hostile:{HERE}:")


def test_a_class_name_that_is_not_a_name_does_not_print():
    planted = type("token 123456:ABC", (Exception,), {})
    assert failure_chain(raised(planted())).startswith(f"Exception:{HERE}:")


def test_a_chain_is_bounded_and_survives_a_cycle():
    first, second = ValueError("x"), ValueError("y")
    first.__context__, second.__context__ = second, first
    assert failure_chain(first).count(">") == 1

    errors = [ValueError(str(number)) for number in range(9)]
    for error, cause in zip(errors, errors[1:]):
        error.__cause__ = cause
    assert failure_chain(errors[0]).count(">") == 3


def test_a_line_is_one_token_and_an_unraised_error_has_no_place():
    assert failure_chain(ValueError("fixed-word")) == "ValueError:fixed-word"
    assert " " not in failure_chain(raised(ValueError("not a fixed word")))
