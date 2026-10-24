"""Persistent Telegram bot bindings, routing identities and delivery records."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class TelegramBotLinkRow(Base):
    __tablename__ = "telegram_bot_links"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    agent_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    agent_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    bot_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bot_username: Mapped[str] = mapped_column(String(64), nullable=False)
    bot_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    encrypted_token: Mapped[str] = mapped_column(Text, nullable=False)
    token_revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    connect_conflict_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    origin: Mapped[str] = mapped_column(String(16), nullable=False, default="pasted")
    manager_link_id: Mapped[str | None] = mapped_column(String(32), ForeignKey("telegram_bot_links.id", ondelete="SET NULL"), nullable=True)
    creator_telegram_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    can_manage_bots: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    bind_mode: Mapped[str] = mapped_column(String(8), nullable=False, default="code")
    connect_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    connection_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("channel_connections.id", ondelete="SET NULL"), nullable=True)
    binding_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    work_baseline_generation: Mapped[int | None] = mapped_column(Integer, nullable=True)
    telegram_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    chat_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    connected_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    health: Mapped[str] = mapped_column(String(24), nullable=False, default="ok")
    conflict_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    probe_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acts_paused_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    limited_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    webhook_host: Mapped[str | None] = mapped_column(String(255), nullable=True)
    topics_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    profile_pending: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    profile_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    profile_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notices: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_ok_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_update_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    connected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("agent_kind IN ('lead_agent', 'custom_agent', 'instance')", name="ck_telegram_bot_links_agent_kind"),
        CheckConstraint("origin IN ('pasted', 'managed')", name="ck_telegram_bot_links_origin"),
        CheckConstraint("bind_mode IN ('code', 'creator')", name="ck_telegram_bot_links_bind_mode"),
        CheckConstraint("state IN ('waiting_connect', 'connected', 'held', 'blocked')", name="ck_telegram_bot_links_state"),
        CheckConstraint("health IN ('ok', 'token_rejected', 'conflict', 'unreachable', 'secret_unreadable', 'agent_unavailable', 'stopped', 'policy_off')", name="ck_telegram_bot_links_health"),
        UniqueConstraint("bot_id", name="uq_telegram_bot_links_bot"),
        UniqueConstraint("owner_user_id", "agent_kind", "agent_ref", name="uq_telegram_bot_links_owner_agent"),
        Index("ix_telegram_bot_links_owner", "owner_user_id"),
        Index("ix_telegram_bot_links_agent", "agent_kind", "agent_ref", "state"),
    )


class TelegramBotLinkEventRow(Base):
    __tablename__ = "telegram_bot_link_events"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    link_id: Mapped[str] = mapped_column(String(32), nullable=False)
    owner_user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    bot_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    actor_id: Mapped[str] = mapped_column(String(64), nullable=False)
    operation_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    request_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    action: Mapped[str] = mapped_column(String(24), nullable=False)
    detail: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "action IN ('add', 'created_managed', 'replace_token', 'replace_bot', 'managed_token', 'remove', 'connect', 'disconnect', "
            "'disconnect_all', 'hold', 'release', 'blocked', 'unblocked', 'token_rejected', 'conflict', 'conflict_ended', "
            "'conflict_acknowledged', 'stopped', 'retry', 'rewrap', 'agent_removed', 'managed_given_away', 'policy_off', 'policy_on', 'token_replace_receipt')",
            name="ck_telegram_bot_link_events_action",
        ),
        Index("ix_telegram_bot_link_events_owner", "owner_user_id", "created_at"),
        Index("ix_telegram_bot_link_events_created", "created_at"),
        Index("uq_telegram_link_event_operation", "owner_user_id", "operation_id", unique=True, sqlite_where=text("operation_id IS NOT NULL"), postgresql_where=text("operation_id IS NOT NULL")),
    )


class TelegramAccountRow(Base):
    __tablename__ = "telegram_accounts"

    telegram_user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    owner_user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    first_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_telegram_accounts_owner", "owner_user_id"),)


class TelegramPersonSettingsRow(Base):
    __tablename__ = "telegram_person_settings"

    owner_user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False)
    locale: Mapped[str] = mapped_column(String(8), nullable=False, default="en-US")
    quiet_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    quiet_start_minute: Mapped[int] = mapped_column(Integer, nullable=False, default=1260)
    quiet_end_minute: Mapped[int] = mapped_column(Integer, nullable=False, default=420)
    notify: Mapped[str] = mapped_column(String(12), nullable=False, default="all")
    summary_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    summary_minute: Mapped[int] = mapped_column(Integer, nullable=False, default=480)
    voice_agreed_for: Mapped[str | None] = mapped_column(String(16), nullable=True)
    voice_agreed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("locale IN ('en-US', 'zh-CN')", name="ck_telegram_person_settings_locale"),
        CheckConstraint("quiet_start_minute BETWEEN 0 AND 1439", name="ck_telegram_person_settings_quiet_start"),
        CheckConstraint("quiet_end_minute BETWEEN 0 AND 1439", name="ck_telegram_person_settings_quiet_end"),
        CheckConstraint("notify IN ('all', 'questions', 'none')", name="ck_telegram_person_settings_notify"),
        CheckConstraint("summary_minute BETWEEN 0 AND 1439", name="ck_telegram_person_settings_summary"),
        CheckConstraint("voice_agreed_for IN ('openai', 'gemini')", name="ck_telegram_person_settings_voice"),
    )


class TelegramBotIntentRow(Base):
    __tablename__ = "telegram_bot_intents"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    manager_link_id: Mapped[str] = mapped_column(String(32), ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), nullable=False)
    manager_binding_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    agent_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    agent_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    suggested_username: Mapped[str] = mapped_column(String(32), nullable=False)
    suggested_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(8), nullable=False)
    add_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    bot_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("source IN ('web', 'chat')", name="ck_telegram_bot_intents_source"),
        Index("uq_telegram_bot_intents_open", "owner_user_id", unique=True, sqlite_where=text("consumed_at IS NULL AND cancelled_at IS NULL"), postgresql_where=text("consumed_at IS NULL AND cancelled_at IS NULL")),
    )


class TelegramInstancePolicyRow(Base):
    __tablename__ = "telegram_instance_policy"

    instance_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    policy: Mapped[str] = mapped_column(String(12), nullable=False)
    updated_by: Mapped[str] = mapped_column(String(64), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (CheckConstraint("policy IN ('allowed', 'protected', 'off')", name="ck_telegram_instance_policy_policy"),)


class TelegramAssistantAddRow(Base):
    __tablename__ = "telegram_assistant_adds"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    link_id: Mapped[str | None] = mapped_column(String(32), ForeignKey("telegram_bot_links.id", ondelete="SET NULL"), nullable=True)
    owner_user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    role_id: Mapped[str | None] = mapped_column(String(40), nullable=True)
    name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    definition_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    instance_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    intent_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    state: Mapped[str] = mapped_column(String(20), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("state IN ('draft', 'confirmed', 'definition_created', 'created', 'bot_pending', 'done', 'refused', 'abandoned')", name="ck_telegram_assistant_adds_state"),
        Index("ix_telegram_assistant_adds_owner", "owner_user_id", "state"),
    )


class TelegramWorkTopicRow(Base):
    __tablename__ = "telegram_work_topics"

    link_id: Mapped[str] = mapped_column(String(32), ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), primary_key=True)
    work_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    binding_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    creation_state: Mapped[str] = mapped_column(String(16), nullable=False)
    message_thread_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint("creation_state IN ('creating', 'ready', 'unavailable')", name="ck_telegram_work_topics_creation_state"),
        UniqueConstraint("link_id", "message_thread_id", name="uq_telegram_work_topics_thread"),
    )


class TelegramCallbackRow(Base):
    __tablename__ = "telegram_callbacks"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    group_id: Mapped[str] = mapped_column(String(32), nullable=False)
    link_id: Mapped[str] = mapped_column(String(32), ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    binding_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    message_thread_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    target_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    target_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_reason: Mapped[str | None] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("target_kind IN ('request', 'work', 'attempt', 'thread', 'draft', 'add_assistant', 'link', 'outbox', 'act')", name="ck_telegram_callbacks_target_kind"),
        Index("ix_telegram_callbacks_group", "group_id"),
        Index("ix_telegram_callbacks_message", "link_id", "message_id"),
        Index("ix_telegram_callbacks_target", "link_id", "target_kind", "target_id"),
        Index("ix_telegram_callbacks_expires", "expires_at"),
    )


class TelegramActRow(Base):
    __tablename__ = "telegram_acts"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    link_id: Mapped[str | None] = mapped_column(String(32), ForeignKey("telegram_bot_links.id", ondelete="SET NULL"), nullable=True)
    bot_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    owner_user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    binding_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    chat_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    instance_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    work_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    request_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    operation_ids: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    continuation: Mapped[dict | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    stop_dispatch: Mapped[dict | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    body: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    result: Mapped[dict | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    first_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("state IN ('draft', 'pending', 'waiting', 'done', 'refused', 'conflict', 'uncertain')", name="ck_telegram_acts_state"),
        Index("ix_telegram_acts_state", "state", "updated_at"),
        Index("ix_telegram_acts_owner", "owner_user_id", "created_at"),
        Index("ix_telegram_acts_owner_kind", "owner_user_id", "kind", "created_at"),
        Index("ix_telegram_acts_created", "created_at"),
    )


class TelegramWorkCursorRow(Base):
    __tablename__ = "telegram_work_cursor"

    link_id: Mapped[str] = mapped_column(String(32), ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), primary_key=True)
    work_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(32), nullable=False)
    binding_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_telegram_work_cursor_instance", "instance_id"),
        Index("ix_telegram_work_cursor_link_generation", "link_id", "binding_generation"),
    )


class TelegramOutboxRow(Base):
    __tablename__ = "telegram_outbox"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    link_id: Mapped[str] = mapped_column(String(32), ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), nullable=False)
    owner_user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    dedupe_key: Mapped[str] = mapped_column(String(96), nullable=False)
    class_: Mapped[str] = mapped_column("class", String(16), nullable=False)
    binding_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    instance_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    work_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    request_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    event_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    parts: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    skip_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    not_before: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    silent: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    claim_owner: Mapped[str | None] = mapped_column(String(32), nullable=True)
    claim_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    degraded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failed_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    chat_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    message_thread_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("class IN ('reply', 'question', 'result', 'interrupt', 'info', 'progress', 'summary', 'digest', 'service', 'reminder')", name="ck_telegram_outbox_class"),
        CheckConstraint("state IN ('pending', 'sending', 'sent', 'skipped', 'failed')", name="ck_telegram_outbox_state"),
        UniqueConstraint("link_id", "dedupe_key", name="uq_telegram_outbox_dedupe"),
        Index("ix_telegram_outbox_due", "state", "not_before"),
        Index("ix_telegram_outbox_work", "link_id", "work_id"),
        Index("ix_telegram_outbox_request", "link_id", "request_id"),
        Index("ix_telegram_outbox_message", "link_id", "message_id"),
        Index("ix_telegram_outbox_closed", "closed_at"),
    )


class TelegramInboundMarkRow(Base):
    __tablename__ = "telegram_inbound_marks"

    link_id: Mapped[str] = mapped_column(String(32), ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), primary_key=True)
    message_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    update_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, nullable=False)
    message_thread_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    binding_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    accepted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    route_kind: Mapped[str | None] = mapped_column(String(24), nullable=True)
    route_target_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    route_basis: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    sanitized_input: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    turn_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    grouped_message_ids: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    thread_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    assistant_message_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    operation_ids: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    outbox_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    delivery_ids: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    route_complete: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint("state IN ('accepted', 'dispatched', 'reply_pending', 'done', 'interrupted')", name="ck_telegram_inbound_marks_state"),
        UniqueConstraint("link_id", "update_id", name="uq_telegram_inbound_marks_update"),
        Index("ix_telegram_inbound_marks_closed", "closed_at"),
    )


class TelegramPendingFileRow(Base):
    __tablename__ = "telegram_pending_files"

    link_id: Mapped[str] = mapped_column(String(32), ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), primary_key=True)
    message_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    message_thread_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    binding_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    file_id: Mapped[str] = mapped_column(String(256), nullable=False)
    file_unique_id: Mapped[str] = mapped_column(String(64), nullable=False)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(128), nullable=False)
    size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_image: Mapped[bool] = mapped_column(Boolean, nullable=False)
    agent_file_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TelegramDeploymentRow(Base):
    __tablename__ = "telegram_deployment"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    host_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    polling_state: Mapped[str] = mapped_column(String(24), nullable=False, default="running")
    adopted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("id = 1", name="ck_telegram_deployment_single"),
        CheckConstraint("polling_state IN ('running', 'host_changed', 'polled_elsewhere')", name="ck_telegram_deployment_polling_state"),
    )
