"""jwzthreading.py

Contains an implementation of an algorithm for threading mail
messages, as described at http://www.jwz.org/doc/threading.html.

To use:

  Create a bunch of Message instances, one per message to be threaded,
  filling in the .subject, .message_id, and .references attributes.
  You can use the .message attribute to record the RFC-822 message object,
  or some other piece of information for your own purposes.

  Call the thread() function with a list of the Message instances.

  You'll get back a {subject line -> Container} dictionary; each
  container may have a .children attribute giving descendants of each
  message.  You'll probably want to sort these children by date, subject,
  or some other criterion.

Copyright (c) 2003-2010, A.M. Kuchling.

This code is under a BSD-style license; see the LICENSE file for details.

Python 3 compatibility fixes by Russell Jurney, 2024.
"""

import re
from collections import deque
from typing import Any

__all__ = ["Message", "make_message", "thread"]


class Container:
    """Contains a tree of messages.

    Instance attributes:
      .message : Message
        Message corresponding to this tree node.  This can be None,
        if a Message-Id is referenced but no message with the ID is
        included.

      .children : [Container]
        Possibly-empty list of child containers.

      .parent : Container
        Parent container; may be None.
    """

    def __init__(self) -> None:
        self.message: "Message | None" = None
        self.parent: "Container | None" = None
        self.children: list["Container"] = []

    def __repr__(self) -> str:
        return "<%s %x: %r>" % (self.__class__.__name__, id(self), self.message)

    def is_dummy(self) -> bool:
        return self.message is None

    def add_child(self, child: "Container") -> None:
        if child.parent:
            child.parent.remove_child(child)
        self.children.append(child)
        child.parent = self

    def remove_child(self, child: "Container") -> None:
        self.children.remove(child)
        child.parent = None

    def has_descendant(self, ctr: "Container") -> bool:
        """(Container): bool

        Returns true if 'ctr' is a descendant of this Container.
        """
        stack: deque[Container] = deque()
        stack.append(self)
        seen: set[Container] = set()
        while stack:
            node = stack.pop()
            if node is ctr:
                return True
            seen.add(node)
            for child in node.children:
                if child not in seen:
                    stack.append(child)
        return False


def uniq(alist: list[str]) -> list[str]:
    seen: dict[str, str] = {}
    return [seen.setdefault(e, e) for e in alist if e not in seen.keys()]


msgid_pat = re.compile(r"<([^>]+)>")
restrip_pat = re.compile(
    r"""(
  (Re(\[\d+\])?:) | (\[ [^\]]+ \])
\s*)+
""",
    re.I | re.VERBOSE,
)


def make_message(msg: Any) -> "Message":
    """(msg:rfc822.Message) : Message
    Create a Message object for threading purposes from an RFC822
    message.
    """
    new = Message(msg)

    m = msgid_pat.search(msg.get("Message-ID", ""))
    if m is None:
        raise ValueError("Message does not contain a Message-ID: header")

    new.message_id = m.group(1)

    refs = msg.get("References", "")
    new.references = msgid_pat.findall(refs)
    new.references = uniq(new.references)
    new.subject = msg.get("Subject", "No subject")

    in_reply_to = msg.get("In-Reply-To", "")
    m = msgid_pat.search(in_reply_to)
    if m:
        msg_id = m.group(1)
        if msg_id not in new.references:
            new.references.append(msg_id)

    return new


class Message:
    """Represents a message to be threaded.

    Instance attributes:
    .subject : str
      Subject line of the message.
    .message_id : str
      Message ID as retrieved from the Message-ID header.
    .references : [str]
      List of message IDs from the In-Reply-To and References headers.
    .message : any
      Can contain information for the caller's use.

    """

    __slots__ = ["message", "message_id", "references", "subject"]

    def __init__(self, msg: Any = None) -> None:
        self.message: Any = msg
        self.message_id: str | None = None
        self.references: list[str] = []
        self.subject: str | None = None

    def __repr__(self) -> str:
        return "<%s: %r>" % (self.__class__.__name__, self.message_id)


def prune_container(container: Container) -> list[Container]:
    """(container:Container) : [Container]
    Recursively prune a tree of containers, as described in step 4
    of the algorithm.  Returns a list of the children that should replace
    this container.
    """

    new_children: list[Container] = []
    for ctr in container.children[:]:
        L = prune_container(ctr)
        new_children.extend(L)
        container.remove_child(ctr)

    for c in new_children:
        container.add_child(c)

    if container.message is None and len(container.children) == 0:
        return []
    elif container.message is None and (
        len(container.children) == 1 or container.parent is not None
    ):
        L = container.children[:]
        for c in L:
            container.remove_child(c)
        return L
    else:
        return [container]


def thread(msglist: list[Message]) -> dict[str, Container]:
    """([Message]) : {string:Container}

    The main threading function.  This takes a list of Message
    objects, and returns a dictionary mapping subjects to Containers.
    Containers are trees, with the .children attribute containing a
    list of subtrees, so callers can then sort children by date or
    poster or whatever.
    """

    id_table: dict[str | None, Container] = {}
    for msg in msglist:
        this_container = id_table.get(msg.message_id, None)
        if this_container is not None:
            this_container.message = msg
        else:
            this_container = Container()
            this_container.message = msg
            id_table[msg.message_id] = this_container

        prev: Container | None = None
        for ref in msg.references:
            container = id_table.get(ref, None)
            if container is None:
                container = Container()
                container.message_id = ref  # type: ignore[attr-defined]
                id_table[ref] = container

            if prev is not None:
                if container is this_container:
                    continue
                if container.has_descendant(prev):
                    continue
                prev.add_child(container)

            prev = container

        if prev is not None:
            prev.add_child(this_container)

    root_set = [container for container in id_table.values() if container.parent is None]

    del id_table

    for container in root_set:
        assert container.parent is None

    new_root_set: list[Container] = []
    for container in root_set:
        L = prune_container(container)
        new_root_set.extend(L)

    root_set = new_root_set

    subject_table: dict[str, Container] = {}
    for container in root_set:
        if container.message:
            subj = container.message.subject or ""
        else:
            # Algorithm guarantees children[0].message exists if container.message is None
            subj = container.children[0].message.subject or ""  # type: ignore[union-attr]

        subj = restrip_pat.sub("", subj)
        if subj == "":
            continue

        existing = subject_table.get(subj, None)
        if (
            existing is None
            or (existing.message is not None and container.message is None)
            or (
                existing.message is not None
                and container.message is not None
                and len(existing.message.subject or "") > len(container.message.subject or "")
            )
        ):
            subject_table[subj] = container

    for container in root_set:
        if container.message:
            subj = container.message.subject or ""
        else:
            # Algorithm guarantees children[0].message exists if container.message is None
            subj = container.children[0].message.subject or ""  # type: ignore[union-attr]

        subj = restrip_pat.sub("", subj)
        ctr = subject_table.get(subj)
        if ctr is None or ctr is container:
            continue
        if ctr.is_dummy() and container.is_dummy():
            for c in ctr.children:
                container.add_child(c)
        elif ctr.is_dummy() or container.is_dummy():
            if ctr.is_dummy():
                ctr.add_child(container)
            else:
                container.add_child(ctr)
        # At this point, both ctr.message and container.message are not None (not dummy)
        elif len(ctr.message.subject or "") < len(container.message.subject or ""):  # type: ignore[union-attr]
            ctr.add_child(container)
        elif len(ctr.message.subject or "") > len(container.message.subject or ""):  # type: ignore[union-attr]
            container.add_child(ctr)
        else:
            new = Container()
            new.add_child(ctr)
            new.add_child(container)
            subject_table[subj] = new

    return subject_table
