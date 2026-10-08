"""Feature-owned resource links; labels/namespace keys never confer access."""

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base
from deerflow.spaces.contract import SpaceConflict, SpaceNotFound
from deerflow.utils.file_io import await_drained


class StorageFeatureLinkRow(Base):
    __tablename__ = "hm_storage_feature_links"
    __table_args__ = (UniqueConstraint("space_id", name="uq_hm_storage_feature_links_space"),)
    namespace: Mapped[str] = mapped_column(String(128), primary_key=True)
    key: Mapped[str] = mapped_column(String(192), primary_key=True)
    space_id: Mapped[str] = mapped_column(String(32), ForeignKey("storage_spaces.id"))


class FeatureResources:
    def __init__(self, files, namespace):
        self.files, self.namespace = files, namespace

    async def find(self, key):
        if not isinstance(key, str) or not 1 <= len(key) <= 192:
            raise ValueError("A bounded feature relationship key is required")
        async with self.files.registry._sf() as session:
            row = await session.get(StorageFeatureLinkRow, (self.namespace, key))
            return row.space_id if row else None

    async def get(self, *, actor, key):
        space_id = await self.find(key)
        if space_id is None:
            raise SpaceNotFound("Feature resource has not been provisioned")
        return await self.files.registry.get(actor=actor, space_id=space_id)

    async def ensure(self, *, actor, key, name, custody, mode, feature=None):
        async def perform():
            for _ in range(len(self.files.catalog.volumes) + 1):
                existing = await self.find(key)
                if existing is not None:
                    # Revocation, archive and deletion never lazily allocate a replacement.
                    return await self.files.registry.get(actor=actor, space_id=existing)
                try:
                    async with self.files.provisioning(actor=actor, name=name, custody=custody, mode=mode, feature=feature) as (session, space):
                        session.add(StorageFeatureLinkRow(namespace=self.namespace, key=key, space_id=space.id))
                        await session.flush()
                    return space
                except IntegrityError:
                    continue
            raise SpaceConflict("Concurrent feature provisioning exhausted capacity")

        return await await_drained(perform())
