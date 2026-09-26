"""Rows through which Gateway processes confirm they ended what a refused owner held."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class RefusalCheckRow(Base):
    """One request, made after a refusal committed, that every live process look again."""

    __tablename__ = "refusal_checks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class GatewayProcessRow(Base):
    """A live Gateway process: its heartbeat on the database clock, and the latest check it acted on."""

    __tablename__ = "gateway_processes"

    process_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    checked_through: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class SurfaceEndingRow(Base):
    """What one process ended for one owner, on one surface, acting on one check."""

    __tablename__ = "surface_endings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    process_id: Mapped[str] = mapped_column(String(128), nullable=False)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    surface: Mapped[str] = mapped_column(String(64), nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False)
    check_id: Mapped[int] = mapped_column(Integer, nullable=False)
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_surface_endings_user_check", "user_id", "check_id"),)
