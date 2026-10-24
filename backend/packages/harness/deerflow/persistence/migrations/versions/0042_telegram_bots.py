"""Durable bot bindings, receipts and delivery state. Frozen additive DDL."""

import importlib

import sqlalchemy as sa
from alembic import op

revision = "0042_telegram_bots"
down_revision = "0041_storage_host_replacements"
branch_labels = None
depends_on = None
_metadata = sa.MetaData()
sa.Table("channel_connections", _metadata, sa.Column("id", sa.String(64), primary_key=True))
_tables = []

_table = sa.Table(
    "telegram_bot_links",
    _metadata,
    sa.Column("id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("owner_user_id", sa.String(64), nullable=False),
    sa.Column("agent_kind", sa.String(16), nullable=False),
    sa.Column("agent_ref", sa.String(128), nullable=False),
    sa.Column("bot_id", sa.BigInteger(), nullable=False),
    sa.Column("bot_username", sa.String(64), nullable=False),
    sa.Column("bot_name", sa.String(64), nullable=True),
    sa.Column("encrypted_token", sa.Text(), nullable=False),
    sa.Column("token_revision", sa.Integer(), nullable=False),
    sa.Column("connect_conflict_epoch", sa.BigInteger(), nullable=False),
    sa.Column("origin", sa.String(16), nullable=False),
    sa.Column("manager_link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="SET NULL"), nullable=True),
    sa.Column("creator_telegram_user_id", sa.BigInteger(), nullable=True),
    sa.Column("can_manage_bots", sa.Boolean(), nullable=False),
    sa.Column("bind_mode", sa.String(8), nullable=False),
    sa.Column("connect_expires_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("connection_id", sa.String(64), sa.ForeignKey("channel_connections.id", ondelete="SET NULL"), nullable=True),
    sa.Column("binding_generation", sa.Integer(), nullable=False),
    sa.Column("work_baseline_generation", sa.Integer(), nullable=True),
    sa.Column("telegram_user_id", sa.BigInteger(), nullable=True),
    sa.Column("chat_id", sa.BigInteger(), nullable=True),
    sa.Column("connected_name", sa.String(128), nullable=True),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("health", sa.String(24), nullable=False),
    sa.Column("conflict_since", sa.DateTime(timezone=True), nullable=True),
    sa.Column("probe_until", sa.DateTime(timezone=True), nullable=True),
    sa.Column("acts_paused_since", sa.DateTime(timezone=True), nullable=True),
    sa.Column("limited_since", sa.DateTime(timezone=True), nullable=True),
    sa.Column("webhook_host", sa.String(255), nullable=True),
    sa.Column("topics_enabled", sa.Boolean(), nullable=False),
    sa.Column("profile_pending", sa.Boolean(), nullable=False),
    sa.Column("profile_version", sa.Integer(), nullable=False),
    sa.Column("profile_synced_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("notices", sa.JSON(), nullable=False),
    sa.Column("last_error", sa.String(500), nullable=True),
    sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("last_ok_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("last_update_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("connected_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("agent_kind IN ('lead_agent', 'custom_agent', 'instance')", name="ck_telegram_bot_links_agent_kind"),
    sa.CheckConstraint("origin IN ('pasted', 'managed')", name="ck_telegram_bot_links_origin"),
    sa.CheckConstraint("bind_mode IN ('code', 'creator')", name="ck_telegram_bot_links_bind_mode"),
    sa.CheckConstraint("state IN ('waiting_connect', 'connected', 'held', 'blocked')", name="ck_telegram_bot_links_state"),
    sa.CheckConstraint("health IN ('ok', 'token_rejected', 'conflict', 'unreachable', 'secret_unreadable', 'agent_unavailable', 'stopped', 'policy_off')", name="ck_telegram_bot_links_health"),
    sa.UniqueConstraint("bot_id", name="uq_telegram_bot_links_bot"),
    sa.UniqueConstraint("owner_user_id", "agent_kind", "agent_ref", name="uq_telegram_bot_links_owner_agent"),
)
sa.Index("ix_telegram_bot_links_owner", _table.c.owner_user_id)
sa.Index("ix_telegram_bot_links_agent", _table.c.agent_kind, _table.c.agent_ref, _table.c.state)
_tables.append(_table)

_table = sa.Table(
    "telegram_bot_link_events",
    _metadata,
    sa.Column("id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("link_id", sa.String(32), nullable=False),
    sa.Column("owner_user_id", sa.String(64), nullable=False),
    sa.Column("bot_id", sa.BigInteger(), nullable=False),
    sa.Column("actor_id", sa.String(64), nullable=False),
    sa.Column("operation_id", sa.String(32), nullable=True),
    sa.Column("request_digest", sa.String(64), nullable=True),
    sa.Column("action", sa.String(24), nullable=False),
    sa.Column("detail", sa.JSON(), nullable=False),
    sa.Column("seen_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint(
        "action IN ('add', 'created_managed', 'replace_token', 'replace_bot', 'managed_token', 'remove', 'connect', "
        "'disconnect', 'disconnect_all', 'hold', 'release', 'blocked', 'unblocked', 'token_rejected', 'conflict', "
        "'conflict_ended', 'conflict_acknowledged', 'stopped', 'retry', 'rewrap', 'agent_removed', 'managed_given_away', "
        "'policy_off', 'policy_on', 'token_replace_receipt')",
        name="ck_telegram_bot_link_events_action",
    ),
)
sa.Index("ix_telegram_bot_link_events_owner", _table.c.owner_user_id, _table.c.created_at)
sa.Index("ix_telegram_bot_link_events_created", _table.c.created_at)
sa.Index("uq_telegram_link_event_operation", _table.c.owner_user_id, _table.c.operation_id, unique=True, sqlite_where=sa.text("operation_id IS NOT NULL"), postgresql_where=sa.text("operation_id IS NOT NULL"))
_tables.append(_table)

_table = sa.Table(
    "telegram_accounts",
    _metadata,
    sa.Column("telegram_user_id", sa.BigInteger(), primary_key=True, autoincrement=False, nullable=False),
    sa.Column("owner_user_id", sa.String(64), nullable=False),
    sa.Column("first_name", sa.String(64), nullable=True),
    sa.Column("username", sa.String(64), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
)
sa.Index("ix_telegram_accounts_owner", _table.c.owner_user_id)
_tables.append(_table)

_table = sa.Table(
    "telegram_person_settings",
    _metadata,
    sa.Column("owner_user_id", sa.String(64), primary_key=True, nullable=False),
    sa.Column("timezone", sa.String(64), nullable=False),
    sa.Column("locale", sa.String(8), nullable=False),
    sa.Column("quiet_enabled", sa.Boolean(), nullable=False),
    sa.Column("quiet_start_minute", sa.Integer(), nullable=False),
    sa.Column("quiet_end_minute", sa.Integer(), nullable=False),
    sa.Column("notify", sa.String(12), nullable=False),
    sa.Column("summary_enabled", sa.Boolean(), nullable=False),
    sa.Column("summary_minute", sa.Integer(), nullable=False),
    sa.Column("voice_agreed_for", sa.String(16), nullable=True),
    sa.Column("voice_agreed_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("locale IN ('en-US', 'zh-CN')", name="ck_telegram_person_settings_locale"),
    sa.CheckConstraint("quiet_start_minute BETWEEN 0 AND 1439", name="ck_telegram_person_settings_quiet_start"),
    sa.CheckConstraint("quiet_end_minute BETWEEN 0 AND 1439", name="ck_telegram_person_settings_quiet_end"),
    sa.CheckConstraint("notify IN ('all', 'questions', 'none')", name="ck_telegram_person_settings_notify"),
    sa.CheckConstraint("summary_minute BETWEEN 0 AND 1439", name="ck_telegram_person_settings_summary"),
    sa.CheckConstraint("voice_agreed_for IN ('openai', 'gemini')", name="ck_telegram_person_settings_voice"),
)
_tables.append(_table)

_table = sa.Table(
    "telegram_bot_intents",
    _metadata,
    sa.Column("id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("owner_user_id", sa.String(64), nullable=False),
    sa.Column("manager_link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), nullable=False),
    sa.Column("manager_binding_generation", sa.Integer(), nullable=False),
    sa.Column("agent_kind", sa.String(16), nullable=False),
    sa.Column("agent_ref", sa.String(128), nullable=False),
    sa.Column("suggested_username", sa.String(32), nullable=False),
    sa.Column("suggested_name", sa.String(64), nullable=False),
    sa.Column("source", sa.String(8), nullable=False),
    sa.Column("add_id", sa.String(32), nullable=True),
    sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("bot_id", sa.BigInteger(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("source IN ('web', 'chat')", name="ck_telegram_bot_intents_source"),
)
sa.Index("uq_telegram_bot_intents_open", _table.c.owner_user_id, unique=True, sqlite_where=sa.text("consumed_at IS NULL AND cancelled_at IS NULL"), postgresql_where=sa.text("consumed_at IS NULL AND cancelled_at IS NULL"))
_tables.append(_table)

_table = sa.Table(
    "telegram_instance_policy",
    _metadata,
    sa.Column("instance_id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("policy", sa.String(12), nullable=False),
    sa.Column("updated_by", sa.String(64), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("policy IN ('allowed', 'protected', 'off')", name="ck_telegram_instance_policy_policy"),
)
_tables.append(_table)

_table = sa.Table(
    "telegram_assistant_adds",
    _metadata,
    sa.Column("id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="SET NULL"), nullable=True),
    sa.Column("owner_user_id", sa.String(64), nullable=False),
    sa.Column("role_id", sa.String(40), nullable=True),
    sa.Column("name", sa.String(64), nullable=True),
    sa.Column("definition_name", sa.String(128), nullable=True),
    sa.Column("instance_id", sa.String(32), nullable=True),
    sa.Column("intent_id", sa.String(32), nullable=True),
    sa.Column("state", sa.String(20), nullable=False),
    sa.Column("reason", sa.String(32), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("state IN ('draft', 'confirmed', 'definition_created', 'created', 'bot_pending', 'done', 'refused', 'abandoned')", name="ck_telegram_assistant_adds_state"),
)
sa.Index("ix_telegram_assistant_adds_owner", _table.c.owner_user_id, _table.c.state)
_tables.append(_table)

_table = sa.Table(
    "telegram_work_topics",
    _metadata,
    sa.Column("link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), primary_key=True, nullable=False),
    sa.Column("work_id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("chat_id", sa.BigInteger(), nullable=False),
    sa.Column("binding_generation", sa.Integer(), nullable=False),
    sa.Column("creation_state", sa.String(16), nullable=False),
    sa.Column("message_thread_id", sa.BigInteger(), nullable=True),
    sa.Column("name", sa.String(128), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("creation_state IN ('creating', 'ready', 'unavailable')", name="ck_telegram_work_topics_creation_state"),
    sa.UniqueConstraint("link_id", "message_thread_id", name="uq_telegram_work_topics_thread"),
)
_tables.append(_table)

_table = sa.Table(
    "telegram_callbacks",
    _metadata,
    sa.Column("id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("group_id", sa.String(32), nullable=False),
    sa.Column("link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), nullable=False),
    sa.Column("chat_id", sa.BigInteger(), nullable=False),
    sa.Column("binding_generation", sa.Integer(), nullable=False),
    sa.Column("message_id", sa.BigInteger(), nullable=True),
    sa.Column("message_thread_id", sa.BigInteger(), nullable=True),
    sa.Column("kind", sa.String(24), nullable=False),
    sa.Column("target_kind", sa.String(16), nullable=True),
    sa.Column("target_id", sa.String(64), nullable=True),
    sa.Column("payload", sa.JSON(), nullable=False),
    sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("closed_reason", sa.String(16), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("target_kind IN ('request', 'work', 'attempt', 'thread', 'draft', 'add_assistant', 'link', 'outbox', 'act')", name="ck_telegram_callbacks_target_kind"),
)
sa.Index("ix_telegram_callbacks_group", _table.c.group_id)
sa.Index("ix_telegram_callbacks_message", _table.c.link_id, _table.c.message_id)
sa.Index("ix_telegram_callbacks_target", _table.c.link_id, _table.c.target_kind, _table.c.target_id)
sa.Index("ix_telegram_callbacks_expires", _table.c.expires_at)
_tables.append(_table)

_table = sa.Table(
    "telegram_acts",
    _metadata,
    sa.Column("id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="SET NULL"), nullable=True),
    sa.Column("bot_id", sa.BigInteger(), nullable=False),
    sa.Column("owner_user_id", sa.String(64), nullable=False),
    sa.Column("binding_generation", sa.Integer(), nullable=False),
    sa.Column("telegram_user_id", sa.BigInteger(), nullable=False),
    sa.Column("chat_id", sa.BigInteger(), nullable=True),
    sa.Column("message_id", sa.BigInteger(), nullable=True),
    sa.Column("kind", sa.String(24), nullable=False),
    sa.Column("instance_id", sa.String(32), nullable=True),
    sa.Column("work_id", sa.String(32), nullable=True),
    sa.Column("request_id", sa.String(32), nullable=True),
    sa.Column("operation_ids", sa.JSON(), nullable=False),
    sa.Column("continuation", sa.JSON(none_as_null=True), nullable=True),
    sa.Column("stop_dispatch", sa.JSON(none_as_null=True), nullable=True),
    sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("body", sa.JSON(), nullable=False),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("reason", sa.String(32), nullable=True),
    sa.Column("result", sa.JSON(none_as_null=True), nullable=True),
    sa.Column("attempts", sa.Integer(), nullable=False),
    sa.Column("first_attempt_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("state IN ('draft', 'pending', 'waiting', 'done', 'refused', 'conflict', 'uncertain')", name="ck_telegram_acts_state"),
)
sa.Index("ix_telegram_acts_state", _table.c.state, _table.c.updated_at)
sa.Index("ix_telegram_acts_owner", _table.c.owner_user_id, _table.c.created_at)
sa.Index("ix_telegram_acts_owner_kind", _table.c.owner_user_id, _table.c.kind, _table.c.created_at)
sa.Index("ix_telegram_acts_created", _table.c.created_at)
_tables.append(_table)

_table = sa.Table(
    "telegram_work_cursor",
    _metadata,
    sa.Column("link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), primary_key=True, nullable=False),
    sa.Column("work_id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("instance_id", sa.String(32), nullable=False),
    sa.Column("binding_generation", sa.Integer(), nullable=False),
    sa.Column("revision", sa.Integer(), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
)
sa.Index("ix_telegram_work_cursor_instance", _table.c.instance_id)
sa.Index("ix_telegram_work_cursor_link_generation", _table.c.link_id, _table.c.binding_generation)
_tables.append(_table)

_table = sa.Table(
    "telegram_outbox",
    _metadata,
    sa.Column("id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), nullable=False),
    sa.Column("owner_user_id", sa.String(64), nullable=False),
    sa.Column("dedupe_key", sa.String(96), nullable=False),
    sa.Column("class", sa.String(16), nullable=False),
    sa.Column("binding_generation", sa.Integer(), nullable=False),
    sa.Column("instance_id", sa.String(32), nullable=True),
    sa.Column("work_id", sa.String(32), nullable=True),
    sa.Column("request_id", sa.String(32), nullable=True),
    sa.Column("event_id", sa.String(32), nullable=True),
    sa.Column("payload", sa.JSON(), nullable=False),
    sa.Column("parts", sa.JSON(), nullable=False),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("skip_reason", sa.String(32), nullable=True),
    sa.Column("not_before", sa.DateTime(timezone=True), nullable=False),
    sa.Column("silent", sa.Boolean(), nullable=True),
    sa.Column("attempts", sa.Integer(), nullable=False),
    sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("claim_owner", sa.String(32), nullable=True),
    sa.Column("claim_id", sa.String(32), nullable=True),
    sa.Column("degraded_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("failed_reason", sa.String(32), nullable=True),
    sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("chat_id", sa.BigInteger(), nullable=True),
    sa.Column("message_id", sa.BigInteger(), nullable=True),
    sa.Column("message_thread_id", sa.BigInteger(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("class IN ('reply', 'question', 'result', 'interrupt', 'info', 'progress', 'summary', 'digest', 'service', 'reminder')", name="ck_telegram_outbox_class"),
    sa.CheckConstraint("state IN ('pending', 'sending', 'sent', 'skipped', 'failed')", name="ck_telegram_outbox_state"),
    sa.UniqueConstraint("link_id", "dedupe_key", name="uq_telegram_outbox_dedupe"),
)
sa.Index("ix_telegram_outbox_due", _table.c.state, _table.c.not_before)
sa.Index("ix_telegram_outbox_work", _table.c.link_id, _table.c.work_id)
sa.Index("ix_telegram_outbox_request", _table.c.link_id, _table.c.request_id)
sa.Index("ix_telegram_outbox_message", _table.c.link_id, _table.c.message_id)
sa.Index("ix_telegram_outbox_closed", _table.c.closed_at)
_tables.append(_table)

_table = sa.Table(
    "telegram_inbound_marks",
    _metadata,
    sa.Column("link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), primary_key=True, nullable=False),
    sa.Column("message_id", sa.BigInteger(), primary_key=True, nullable=False),
    sa.Column("update_id", sa.BigInteger(), nullable=False),
    sa.Column("chat_id", sa.BigInteger(), nullable=False),
    sa.Column("message_thread_id", sa.BigInteger(), nullable=True),
    sa.Column("binding_generation", sa.Integer(), nullable=False),
    sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("route_kind", sa.String(24), nullable=True),
    sa.Column("route_target_id", sa.String(64), nullable=True),
    sa.Column("route_basis", sa.JSON(), nullable=False),
    sa.Column("sanitized_input", sa.JSON(), nullable=False),
    sa.Column("turn_id", sa.String(32), nullable=True),
    sa.Column("grouped_message_ids", sa.JSON(), nullable=False),
    sa.Column("thread_id", sa.String(64), nullable=True),
    sa.Column("run_id", sa.String(64), nullable=True),
    sa.Column("assistant_message_id", sa.String(64), nullable=True),
    sa.Column("operation_ids", sa.JSON(), nullable=False),
    sa.Column("outbox_id", sa.String(32), nullable=True),
    sa.Column("delivery_ids", sa.JSON(), nullable=False),
    sa.Column("route_complete", sa.Boolean(), nullable=False),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("state IN ('accepted', 'dispatched', 'reply_pending', 'done', 'interrupted')", name="ck_telegram_inbound_marks_state"),
    sa.UniqueConstraint("link_id", "update_id", name="uq_telegram_inbound_marks_update"),
)
sa.Index("ix_telegram_inbound_marks_closed", _table.c.closed_at)
_tables.append(_table)

_table = sa.Table(
    "telegram_pending_files",
    _metadata,
    sa.Column("link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), primary_key=True, nullable=False),
    sa.Column("message_id", sa.BigInteger(), primary_key=True, nullable=False),
    sa.Column("chat_id", sa.BigInteger(), nullable=False),
    sa.Column("message_thread_id", sa.BigInteger(), nullable=True),
    sa.Column("binding_generation", sa.Integer(), nullable=False),
    sa.Column("file_id", sa.String(256), nullable=False),
    sa.Column("file_unique_id", sa.String(64), nullable=False),
    sa.Column("filename", sa.String(255), nullable=False),
    sa.Column("mime_type", sa.String(128), nullable=False),
    sa.Column("size", sa.BigInteger(), nullable=False),
    sa.Column("is_image", sa.Boolean(), nullable=False),
    sa.Column("agent_file_path", sa.String(1024), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
)
_tables.append(_table)

_table = sa.Table(
    "telegram_deployment",
    _metadata,
    sa.Column("id", sa.Integer(), primary_key=True, autoincrement=False, nullable=False),
    sa.Column("host_id", sa.String(128), nullable=True),
    sa.Column("polling_state", sa.String(24), nullable=False),
    sa.Column("adopted_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("id = 1", name="ck_telegram_deployment_single"),
    sa.CheckConstraint("polling_state IN ('running', 'host_changed', 'polled_elsewhere')", name="ck_telegram_deployment_polling_state"),
)
_tables.append(_table)

_GUARDED = ("telegram_bot_links", "telegram_bot_link_events", "telegram_acts", "telegram_outbox", "telegram_instance_policy", "telegram_assistant_adds")


def upgrade():
    bind = op.get_bind()
    frozen = importlib.import_module("deerflow.persistence.migrations.versions.0035_agent_conversations")
    indexes = importlib.import_module("deerflow.persistence.migrations.versions.0038_agent_work")
    for table in _tables:
        inspector = sa.inspect(bind)
        if table.name in inspector.get_table_names():
            if table.name == "telegram_inbound_marks" and inspector.get_pk_constraint(table.name)["constrained_columns"] == ["link_id", "message_id", "chat_id"]:
                # A legacy stamp may coexist with newer create_all tables.
                # Recognize only the independently frozen immediate successor;
                # This changes no 0042 creation DDL or retained data.
                successor = importlib.import_module("deerflow.persistence.migrations.versions.0043_telegram_inbox_chat_scope")
                successor._validate(bind, successor._after)
            else:
                frozen._validate_existing(inspector, table)
                indexes._validate_indexes(bind, table)
        else:
            table.create(bind)


def downgrade():
    bind = op.get_bind()
    existing = set(sa.inspect(bind).get_table_names())
    for table in _tables:
        if table.name in _GUARDED and table.name in existing and bind.execute(sa.select(sa.literal(1)).select_from(table).limit(1)).first() is not None:
            raise RuntimeError("Refusing to erase Telegram bot links, tokens or history")
    for table in reversed(_tables):
        if table.name in existing:
            table.drop(bind)
