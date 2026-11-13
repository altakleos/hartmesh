"""Add an explicit caller's proof to every source admission in its drained operation."""

from contextlib import contextmanager
from contextvars import ContextVar

_factories = ContextVar("telegram_attention_proofs", default=())


@contextmanager
def attention_proof(factory):
    token = _factories.set((*_factories.get(), factory))
    try:
        yield
    finally:
        _factories.reset(token)


def scoped_projection(source):
    proofs = [factory() for factory in _factories.get()]
    return _ScopedProjection(proofs, source) if proofs else source


class _ScopedProjection:
    def __init__(self, proofs, source):
        self.proofs, self.source = proofs, source

    def extend(self, query):
        query = self.source.extend(query) if self.source is not None else query
        self.source_width = len(query.column_descriptions)
        for proof in self.proofs:
            query = proof.extend(query)
        return query

    def check_with_permissions(self, rows, held):
        for proof in self.proofs:
            proof.check(rows)
        if self.source is not None:
            # Existing owners use both fixed offsets and trailing columns.
            # Preserve their exact row shape while sharing this final snapshot.
            source_rows = [row[: self.source_width] for row in rows]
            qualified = getattr(self.source, "check_with_permissions", None)
            if qualified is None:
                self.source.check(source_rows)
            else:
                qualified(source_rows, held)
