"""Tests for email threading module."""

from typing import Any

from upside.parse.thread import create_jwz_message, thread_emails_local


class TestCreateJwzMessage:
    """Tests for create_jwz_message function."""

    def test_creates_message_with_all_fields(self) -> None:
        """Test that message is created with all fields."""
        row = {
            "id": "<msg1@example.com>",
            "normalized_subject": "test subject",
            "references": ["<ref1@example.com>", "<ref2@example.com>"],
            "in_reply_to": "<parent@example.com>",
        }
        msg = create_jwz_message(row)
        assert msg.message_id == "<msg1@example.com>"
        assert msg.subject == "test subject"
        assert msg.references == [
            "<parent@example.com>",
            "<ref1@example.com>",
            "<ref2@example.com>",
        ]
        assert msg.message == row

    def test_handles_none_id(self) -> None:
        """Test that None id becomes empty string."""
        row: dict[str, Any] = {
            "id": None,
            "normalized_subject": "test",
            "references": [],
            "in_reply_to": None,
        }
        msg = create_jwz_message(row)
        assert msg.message_id == ""

    def test_handles_none_subject(self) -> None:
        """Test that None subject becomes empty string."""
        row: dict[str, Any] = {
            "id": "<msg@example.com>",
            "normalized_subject": None,
            "references": [],
            "in_reply_to": None,
        }
        msg = create_jwz_message(row)
        assert msg.subject == ""

    def test_handles_none_references(self) -> None:
        """Test that None references becomes empty list."""
        row = {
            "id": "<msg@example.com>",
            "normalized_subject": "test",
            "references": None,
            "in_reply_to": None,
        }
        msg = create_jwz_message(row)
        assert msg.references == []

    def test_adds_in_reply_to_to_references(self) -> None:
        """Test that in_reply_to is prepended to references."""
        row = {
            "id": "<msg@example.com>",
            "normalized_subject": "test",
            "references": ["<ref1@example.com>"],
            "in_reply_to": "<parent@example.com>",
        }
        msg = create_jwz_message(row)
        assert msg.references[0] == "<parent@example.com>"

    def test_does_not_duplicate_in_reply_to(self) -> None:
        """Test that in_reply_to is not duplicated if already in references."""
        row = {
            "id": "<msg@example.com>",
            "normalized_subject": "test",
            "references": ["<parent@example.com>", "<ref1@example.com>"],
            "in_reply_to": "<parent@example.com>",
        }
        msg = create_jwz_message(row)
        assert msg.references.count("<parent@example.com>") == 1


class TestThreadEmailsLocal:
    """Tests for thread_emails_local function."""

    def test_handles_empty_list(self) -> None:
        """Test that empty list returns empty list."""
        assert thread_emails_local([]) == []

    def test_threads_single_email(self) -> None:
        """Test threading a single email."""
        rows = [
            {
                "id": "<msg1@example.com>",
                "normalized_subject": "test",
                "references": None,
                "in_reply_to": None,
            }
        ]
        result = thread_emails_local(rows)
        assert len(result) == 1
        assert result[0]["jwz_thread_id"] == "<msg1@example.com>"
        assert result[0]["thread_depth"] == 0
        assert result[0]["parent_id"] is None

    def test_threads_reply_chain(self) -> None:
        """Test threading a reply chain."""
        rows: list[dict[str, Any]] = [
            {
                "id": "<msg1@example.com>",
                "normalized_subject": "test",
                "references": None,
                "in_reply_to": None,
            },
            {
                "id": "<msg2@example.com>",
                "normalized_subject": "test",
                "references": ["<msg1@example.com>"],
                "in_reply_to": "<msg1@example.com>",
            },
        ]
        result = thread_emails_local(rows)
        assert len(result) == 2

        # Both should have same thread ID
        thread_ids = set(r["jwz_thread_id"] for r in result)
        assert len(thread_ids) == 1

    def test_adds_threading_fields(self) -> None:
        """Test that threading fields are added to results."""
        rows = [
            {
                "id": "<msg1@example.com>",
                "normalized_subject": "test",
                "references": None,
                "in_reply_to": None,
                "subject": "Original Subject",
            }
        ]
        result = thread_emails_local(rows)
        assert "jwz_thread_id" in result[0]
        assert "thread_depth" in result[0]
        assert "parent_id" in result[0]
        # Original fields are preserved
        assert result[0]["subject"] == "Original Subject"
