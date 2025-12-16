"""Extract structured fields from email messages using mail-parser."""

import hashlib
import re
import traceback
from typing import Any, Optional

from mailparser import MailParser
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType

# Schema for extracted email fields (includes error field for tracking parse failures)
EMAIL_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("date", TimestampType(), nullable=True),
        StructField("subject", StringType(), nullable=True),
        StructField("from", StringType(), nullable=True),
        StructField("from_name", StringType(), nullable=True),
        StructField("to", ArrayType(StringType()), nullable=True),
        StructField("cc", ArrayType(StringType()), nullable=True),
        StructField("bcc", ArrayType(StringType()), nullable=True),
        StructField("body_clean", StringType(), nullable=True),
        StructField("references", ArrayType(StringType()), nullable=True),
        StructField("in_reply_to", StringType(), nullable=True),
        StructField("normalized_subject", StringType(), nullable=True),
        StructField("parse_error", StringType(), nullable=True),
    ]
)


def normalize_subject(subject: Optional[str]) -> str:
    """Normalize subject by removing Re:, Fwd:, etc. prefixes.

    Parameters
    ----------
    subject : str or None
        Original subject line.

    Returns
    -------
    str
        Normalized subject for threading comparison.
    """
    if not subject:
        return ""

    # Remove common prefixes (case insensitive)
    # Handles: Re:, RE:, Fwd:, FWD:, Fw:, FW:, and variations with []
    normalized = re.sub(
        r"^(\s*(re|fwd?|fw)\s*:\s*|\s*\[.*?\]\s*)+",
        "",
        subject,
        flags=re.IGNORECASE,
    )
    # Normalize whitespace and lowercase
    normalized = " ".join(normalized.lower().split())
    return normalized


def extract_emails_from_tuples(tuples: list[tuple[str, str]]) -> list[str]:
    """Extract email addresses from mail-parser tuple format.

    Parameters
    ----------
    tuples : list
        List of (name, email) tuples from mail-parser.

    Returns
    -------
    list[str]
        List of lowercase email addresses.
    """
    emails = []
    for name, email in tuples:
        if email and email.strip():
            emails.append(email.lower().strip())
    return emails


def parse_references(ref_string: Optional[str]) -> list[str]:
    """Parse References header into list of message IDs.

    Parameters
    ----------
    ref_string : str or None
        References header value.

    Returns
    -------
    list[str]
        List of message IDs.
    """
    if not ref_string:
        return []

    # Extract all <message-id> patterns
    return re.findall(r"<[^>]+>", ref_string)


def parse_email_message(message: str) -> tuple[Any, ...]:
    """Parse a raw email message and extract structured fields.

    Parameters
    ----------
    message : str
        Raw email message content.

    Returns
    -------
    tuple
        Tuple matching EMAIL_SCHEMA fields, including parse_error field.
    """
    try:
        mail = MailParser.from_string(message)

        # Extract from address
        from_email = None
        from_name = None
        if mail.from_ and len(mail.from_) > 0:
            from_name, from_email = mail.from_[0]
            if from_email:
                from_email = from_email.lower().strip()
            if from_name:
                from_name = from_name.strip()

        # Extract to, cc, bcc
        to_emails = extract_emails_from_tuples(mail.to) if mail.to else []
        cc_emails = extract_emails_from_tuples(mail.cc) if mail.cc else []
        bcc_emails = extract_emails_from_tuples(mail.bcc) if mail.bcc else []

        # Get body - prefer plain text
        body_clean = ""
        if mail.text_plain:
            body_clean = "\n".join(mail.text_plain).strip()
        elif mail.body:
            body_clean = mail.body.strip()

        # Get threading headers
        references = parse_references(mail.headers.get("References"))
        in_reply_to = mail.headers.get("In-Reply-To")
        if in_reply_to:
            in_reply_to = in_reply_to.strip()

        # Normalize subject for threading
        normalized_subj = normalize_subject(mail.subject)

        # Convert date to timestamp
        date_val = mail.date if mail.date else None

        return (
            mail.message_id,
            date_val,
            mail.subject,
            from_email,
            from_name,
            to_emails if to_emails else None,
            cc_emails if cc_emails else None,
            bcc_emails if bcc_emails else None,
            body_clean if body_clean else None,
            references if references else None,
            in_reply_to,
            normalized_subj,
            None,  # No error
        )
    except Exception as e:
        # Return error message on parse failure
        error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        return (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            error_msg,
        )


def generate_thread_id(
    email_id: Optional[str],
    references: Optional[list[str]],
    in_reply_to: Optional[str],
    normalized_subject: Optional[str],
) -> str:
    """Generate a thread ID for an email.

    Threading logic:
    1. If References header exists, use the first (root) message ID
    2. Else if In-Reply-To exists, use that
    3. Else if we have the email id and it's not a reply, use id
    4. Else hash the normalized subject

    Parameters
    ----------
    email_id : str or None
        This email's Message-ID (id field).
    references : list[str] or None
        List of referenced message IDs.
    in_reply_to : str or None
        In-Reply-To message ID.
    normalized_subject : str or None
        Normalized subject line.

    Returns
    -------
    str
        Thread ID for grouping related emails.
    """
    # Use References header - first message is the thread root
    if references and len(references) > 0:
        return references[0]

    # Use In-Reply-To as thread identifier
    if in_reply_to:
        return in_reply_to

    # For non-replies, use email id as thread root
    if email_id:
        return email_id

    # Fallback: hash normalized subject
    if normalized_subject:
        return f"subj:{hashlib.md5(normalized_subject.encode()).hexdigest()[:16]}"

    return "unknown"


def extract_email_fields(emails_df: DataFrame) -> DataFrame:
    """Extract structured fields from raw email messages.

    Parameters
    ----------
    emails_df : DataFrame
        DataFrame with 'file' and 'message' columns.

    Returns
    -------
    DataFrame
        DataFrame with extracted email fields and thread_id.
    """
    # Register UDFs
    parse_udf = F.udf(parse_email_message, EMAIL_SCHEMA)
    thread_udf = F.udf(generate_thread_id, StringType())

    # Parse emails and extract fields
    parsed = emails_df.withColumn("parsed", parse_udf(F.col("message")))

    # Expand parsed struct into columns
    extracted = parsed.select(
        F.col("file"),
        F.col("message"),
        F.col("parsed.id").alias("id"),
        F.col("parsed.date").alias("date"),
        F.col("parsed.subject").alias("subject"),
        F.col("parsed.from").alias("from"),
        F.col("parsed.from_name").alias("from_name"),
        F.col("parsed.to").alias("to"),
        F.col("parsed.cc").alias("cc"),
        F.col("parsed.bcc").alias("bcc"),
        F.col("parsed.body_clean").alias("body_clean"),
        F.col("parsed.references").alias("references"),
        F.col("parsed.in_reply_to").alias("in_reply_to"),
        F.col("parsed.normalized_subject").alias("normalized_subject"),
        F.col("parsed.parse_error").alias("parse_error"),
    )

    # Generate thread IDs
    result = extracted.withColumn(
        "thread_id",
        thread_udf(
            F.col("id"),
            F.col("references"),
            F.col("in_reply_to"),
            F.col("normalized_subject"),
        ),
    )

    return result
