"""Album JSON keys must remain typed parameters for PostgreSQL's variadic API."""

import pytest
from sqlalchemy import select
from sqlalchemy.dialects.postgresql.asyncpg import dialect
from sqlalchemy.orm import aliased
from sqlalchemy.sql.sqltypes import NullType

from app.telegram_bots.work_album_guard import AlbumProjection
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.parametrize("model", [TelegramInboundMarkRow, TelegramCallbackRow, TelegramOutboxRow])
def test_album_projection_json_keys_have_postgres_parameter_types(model):
    projection = AlbumProjection(None, sqlite=False)
    query = select(projection.aggregate(aliased(model), model))
    compiled = query.compile(dialect=dialect())
    # json_build_object accepts arbitrary types, so the server cannot infer an
    # untyped key even when its table has no matching rows. In particular, a
    # quoted SQL identifier such as `class` must be encoded as a text value.
    assert set(compiled.params.values()) == {str(column.name) for column in model.__table__.columns}
    assert all(not isinstance(bind.type, NullType) for bind in compiled.binds.values())
