"""Tests for email extraction module."""

from upside.parse.extract import (
    extract_emails_from_tuples,
    generate_thread_id,
    normalize_subject,
    parse_references,
)


class TestNormalizeSubject:
    """Tests for normalize_subject function."""

    def test_removes_re_prefix(self) -> None:
        """Test that Re: prefix is removed."""
        assert normalize_subject("Re: Hello") == "hello"
        assert normalize_subject("RE: Hello") == "hello"
        assert normalize_subject("re: Hello") == "hello"

    def test_removes_fwd_prefix(self) -> None:
        """Test that Fwd: prefix is removed."""
        assert normalize_subject("Fwd: Hello") == "hello"
        assert normalize_subject("FWD: Hello") == "hello"
        assert normalize_subject("Fw: Hello") == "hello"

    def test_removes_multiple_prefixes(self) -> None:
        """Test that multiple prefixes are removed."""
        assert normalize_subject("Re: Re: Hello") == "hello"
        assert normalize_subject("Fwd: Re: Hello") == "hello"
        assert normalize_subject("Re: Fwd: Re: Hello") == "hello"

    def test_keeps_numbered_re(self) -> None:
        """Test that Re[2]: style prefixes are NOT removed (unsupported)."""
        # Note: The current implementation does not strip Re[2]: style prefixes
        assert normalize_subject("Re[2]: Hello") == "re[2]: hello"
        assert normalize_subject("Re[10]: Hello") == "re[10]: hello"

    def test_removes_bracketed_prefixes(self) -> None:
        """Test that [TAG] prefixes are removed."""
        assert normalize_subject("[EXTERNAL] Hello") == "hello"
        assert normalize_subject("[SPAM] Re: Hello") == "hello"

    def test_normalizes_whitespace(self) -> None:
        """Test that whitespace is normalized."""
        assert normalize_subject("  Hello   World  ") == "hello world"

    def test_lowercases_subject(self) -> None:
        """Test that subject is lowercased."""
        assert normalize_subject("HELLO WORLD") == "hello world"

    def test_handles_none(self) -> None:
        """Test that None returns empty string."""
        assert normalize_subject(None) == ""

    def test_handles_empty_string(self) -> None:
        """Test that empty string returns empty string."""
        assert normalize_subject("") == ""


class TestExtractEmailsFromTuples:
    """Tests for extract_emails_from_tuples function."""

    def test_extracts_emails(self) -> None:
        """Test basic email extraction."""
        tuples = [("John Doe", "john@example.com"), ("Jane Doe", "jane@example.com")]
        result = extract_emails_from_tuples(tuples)
        assert result == ["john@example.com", "jane@example.com"]

    def test_lowercases_emails(self) -> None:
        """Test that emails are lowercased."""
        tuples = [("John", "JOHN@EXAMPLE.COM")]
        result = extract_emails_from_tuples(tuples)
        assert result == ["john@example.com"]

    def test_strips_whitespace(self) -> None:
        """Test that whitespace is stripped."""
        tuples = [("John", "  john@example.com  ")]
        result = extract_emails_from_tuples(tuples)
        assert result == ["john@example.com"]

    def test_skips_empty_emails(self) -> None:
        """Test that empty emails are skipped."""
        tuples = [("John", "john@example.com"), ("Empty", ""), ("Whitespace", "   ")]
        result = extract_emails_from_tuples(tuples)
        assert result == ["john@example.com"]

    def test_handles_empty_list(self) -> None:
        """Test that empty list returns empty list."""
        assert extract_emails_from_tuples([]) == []


class TestParseReferences:
    """Tests for parse_references function."""

    def test_parses_single_reference(self) -> None:
        """Test parsing a single message ID."""
        result = parse_references("<abc123@example.com>")
        assert result == ["<abc123@example.com>"]

    def test_parses_multiple_references(self) -> None:
        """Test parsing multiple message IDs."""
        ref_string = "<abc@example.com> <def@example.com> <ghi@example.com>"
        result = parse_references(ref_string)
        assert result == ["<abc@example.com>", "<def@example.com>", "<ghi@example.com>"]

    def test_handles_newlines(self) -> None:
        """Test parsing references with newlines."""
        ref_string = "<abc@example.com>\n\t<def@example.com>"
        result = parse_references(ref_string)
        assert result == ["<abc@example.com>", "<def@example.com>"]

    def test_handles_none(self) -> None:
        """Test that None returns empty list."""
        assert parse_references(None) == []

    def test_handles_empty_string(self) -> None:
        """Test that empty string returns empty list."""
        assert parse_references("") == []

    def test_handles_no_angle_brackets(self) -> None:
        """Test that strings without angle brackets return empty list."""
        assert parse_references("abc123@example.com") == []


class TestGenerateThreadId:
    """Tests for generate_thread_id function."""

    def test_uses_first_reference(self) -> None:
        """Test that first reference is used as thread ID."""
        result = generate_thread_id(
            email_id="<msg1@example.com>",
            references=["<root@example.com>", "<parent@example.com>"],
            in_reply_to="<parent@example.com>",
            normalized_subject="hello",
        )
        assert result == "<root@example.com>"

    def test_uses_in_reply_to_when_no_references(self) -> None:
        """Test that in_reply_to is used when no references."""
        result = generate_thread_id(
            email_id="<msg1@example.com>",
            references=None,
            in_reply_to="<parent@example.com>",
            normalized_subject="hello",
        )
        assert result == "<parent@example.com>"

    def test_uses_message_id_when_no_references_or_reply(self) -> None:
        """Test that message_id is used for root messages."""
        result = generate_thread_id(
            email_id="<msg1@example.com>",
            references=None,
            in_reply_to=None,
            normalized_subject="hello",
        )
        assert result == "<msg1@example.com>"

    def test_uses_subject_hash_as_fallback(self) -> None:
        """Test that subject hash is used as fallback."""
        result = generate_thread_id(
            email_id=None,
            references=None,
            in_reply_to=None,
            normalized_subject="hello world",
        )
        assert result.startswith("subj:")
        assert len(result) == 21  # "subj:" + 16 hex chars

    def test_returns_unknown_when_all_none(self) -> None:
        """Test that 'unknown' is returned when all fields are None."""
        result = generate_thread_id(
            email_id=None,
            references=None,
            in_reply_to=None,
            normalized_subject=None,
        )
        assert result == "unknown"

    def test_empty_references_uses_in_reply_to(self) -> None:
        """Test that empty references list falls back to in_reply_to."""
        result = generate_thread_id(
            email_id="<msg1@example.com>",
            references=[],
            in_reply_to="<parent@example.com>",
            normalized_subject="hello",
        )
        assert result == "<parent@example.com>"
